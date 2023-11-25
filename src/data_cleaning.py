from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from datetime import timedelta
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode
import logging
from aux_functions import count_null_values
from pyspark.sql import Window
from pyspark.sql.functions import col, when

import data_ingestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_hours(start, end):
    """
    Generate a list of hours between two timestamps.

    :param start: Start timestamp
    :param end: End timestamp

    :return: List of hours between the two timestamps
    """
    hours = [
        (start + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
        for i in range(int((end - start).total_seconds() / 3600) + 1)
    ]
    return hours


def add_missing_timestamps(df: DataFrame) -> DataFrame:
    """
    Add missing timestamps to a Spark DataFrame.
    If a timestamp is missing for a turbine, the corresponding row will be filled with null values.F

    :param df: Spark DataFrame

    :return: Spark DataFrame
    """
    turbine_time_ranges = df.groupBy("turbine_id").agg(
        F.min("timestamp").alias("start_date"), F.max("timestamp").alias("end_date")
    )

    generate_hours_udf = udf(generate_hours, ArrayType(StringType()))

    turbine_time_ranges = turbine_time_ranges.withColumn(
        "hours", generate_hours_udf("start_date", "end_date")
    )

    reference_df = turbine_time_ranges.select(
        "turbine_id", explode("hours").alias("timestamp")
    )

    reference_df = reference_df.withColumn("timestamp", F.to_timestamp("timestamp"))
    joined_df = reference_df.join(df, on=["turbine_id", "timestamp"], how="left_outer")

    return joined_df


def add_missing_group_columns(df, group_col, order_col, fill_col):
    """
    Add necessary columns to a Spark DataFrame to fill null values in groups.
    It calculates:
        - prev_value: The previous value of the column to fill
        - next_value: The next value of the column to fill
        - diff: The difference between the previous and next value
        - missing_seq: A column to indicate if the value is missing
        - change: A column to indicate if there is a change in the missing_seq column
        - group_identifier: A column to identify groups of consecutive missing values
        - cum_sum: A cumulative sum of missing values, used to see if there are consecutive missing values
        - step: The step (increase in value) to fill missing values with

    :param df: Spark DataFrame
    :param group_col: Column to group by
    :param order_col: Column to order by
    :param fill_col: Column to fill null values in

    :return: Spark DataFrame
    """
    windowSpec = Window.partitionBy(group_col).orderBy(order_col)
    windowSpecBackward = windowSpec.rowsBetween(Window.unboundedPreceding, 0)
    windowSpecForward = windowSpec.rowsBetween(0, Window.unboundedFollowing)

    df = df.withColumn(
        "prev_value", F.last(fill_col, ignorenulls=True).over(windowSpecBackward)
    )

    df = df.withColumn(
        "next_value", F.first(fill_col, ignorenulls=True).over(windowSpecForward)
    )

    # For the first and last rows, fill the previous/next value with the next/previous value
    df = df.withColumn(
        "prev_value",
        when(
            col("prev_value").isNull() & col(fill_col).isNull(), col("next_value")
        ).otherwise(col("prev_value")),
    )
    df = df.withColumn(
        "next_value",
        when(
            col("next_value").isNull() & col(fill_col).isNull(), col("prev_value")
        ).otherwise(col("next_value")),
    )

    df = df.withColumn("diff", (F.col("next_value") - F.col("prev_value")))

    df = df.withColumn(
        "missing_seq", F.when(F.col(fill_col).isNull(), F.lit(1)).otherwise(F.lit(0))
    )

    df = df.withColumn(
        "change",
        F.when(
            (F.lag("missing_seq", 1, 0).over(windowSpec) != F.col("missing_seq")),
            1,
        ).otherwise(0),
    )

    df = df.withColumn(
        "group_identifier",
        F.sum("change").over(windowSpec),
    )

    windowSpecMissing = Window.partitionBy(group_col, "group_identifier")
    windowSpecMissingBackward = windowSpecMissing.orderBy(order_col).rowsBetween(
        Window.unboundedPreceding, 0
    )
    windowSpecMissingForward = windowSpecMissing.orderBy(order_col).rowsBetween(
        0, Window.unboundedFollowing
    )
    df = df.withColumn("cum_sum", F.sum("missing_seq").over(windowSpecMissingBackward))

    df = df.withColumn(
        "step", F.col("diff") / (F.max("cum_sum").over(windowSpecMissingForward) + 1)
    )
    return df


def fill_missing_in_groups(df, group_col, order_col, fill_col):
    """
    Fill null values in a Spark DataFrame.
    If more than one consecutive null value is found, the missing values will be filled with an ascending/descending sequence of values.

    :param df: Spark DataFrame
    :param group_col: Column to group by
    :param order_col: Column to order by
    :param fill_col: Column to fill null values in

    :return: Spark DataFrame
    """
    df = add_missing_group_columns(df, group_col, order_col, fill_col)

    df = df.withColumn(
        f"{fill_col}",
        F.when(
            F.col(fill_col).isNull(),
            F.col("prev_value") + F.col("step") * F.col("cum_sum"),
        ).otherwise(F.col(fill_col)),
    )
    return df


def fill_missing_values(
    df: DataFrame,
    columns: list = ["wind_speed", "wind_direction", "power_output"],
    order_col="timestamp",
    group_col="turbine_id",
) -> DataFrame:
    """
    Fill missing values in a Spark DataFrame.

    :param df: Spark DataFrame
    :param columns: Columns to fill missing values in

    :return: Spark DataFrame
    """
    for col in columns:
        df = fill_missing_in_groups(
            df=df, group_col=group_col, order_col=order_col, fill_col=col
        )
    return df


def get_bounds(df, col):
    """
    Calculate the lower and upper bounds of a column in a Spark DataFrame, using the IQR method.

    :param df: Spark DataFrame
    :param col: Column to calculate bounds for

    :return: Tuple containing the lower and upper bounds
    """
    quantiles = df.approxQuantile(col, [0.25, 0.75], 0.05)
    IQR = quantiles[1] - quantiles[0]
    lower_bound = quantiles[0] - 1.5 * IQR
    upper_bound = quantiles[1] + 1.5 * IQR
    return lower_bound, upper_bound


def remove_outliers(
    df: DataFrame,
    columns_to_clean=["wind_speed", "wind_direction", "power_output"],
) -> DataFrame:
    """
    Remove outliers from a Spark DataFrame using the IQR method.

    :param df: Spark DataFrame
    :param columns_to_clean: Columns to remove outliers from
    :return: Spark DataFrame
    """
    bounds = {}
    for col in columns_to_clean:
        bounds = get_bounds(df, col)
        df = df.withColumn(
            col,
            F.when(
                (F.col(col) < bounds[0]) | (F.col(col) > bounds[1]), F.lit(None)
            ).otherwise(F.col(col)),
        )
    return df


def clean_data(df: DataFrame) -> DataFrame:
    """
    Perform data cleaning operations on a DataFrame.
    First, it adds missing timestamps to the DataFrame.
    Then, it removes outliers from the DataFrame, to treat them as missing values.
    Finally, it fills missing values in the DataFrame, using the previous and next values in the same turbine.
    If more than one consecutive null value is found, the missing values will be filled with an ascending/descending sequence of values.

    :param df: Spark DataFrame.

    :return: Cleaned DataFrame.
    """
    logger.info(f"Count of null values (beggining): {count_null_values(df)}")

    df = add_missing_timestamps(df)
    logger.info(
        f"Count of null values (after adding missing timestamps): {count_null_values(df)}"
    )

    df_no_outliers = remove_outliers(df)
    logger.info(
        f"Count of null values (after removing outliers): {count_null_values(df_no_outliers)}"
    )

    df_cleaned = fill_missing_values(df)
    logger.info(
        f"Count of null values (after filling missing values): {count_null_values(df_cleaned)}"
    )

    return df_cleaned


def main():
    df = data_ingestion.main()
    logger.info(f"Count of null values: {count_null_values(df)}")
    df_cleaned = clean_data(df)
    return df_cleaned


if __name__ == "__main__":
    df = main()
    df.show()
