from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, count, when, col
import pyspark.sql.functions as F


def count_null_values(df: DataFrame) -> int:
    """
    Count the number of null values in a Spark DataFrame.

    :param df: Spark DataFrame
    :return: Number of null values
    """

    null_counts = df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    )
    null_counts = null_counts.withColumn(
        "null_sum", sum(null_counts[col] for col in null_counts.columns)
    )
    total_nulls = null_counts.first()["null_sum"]
    return total_nulls


def clean_columns(
    df,
    columns=["turbine_id", "timestamp", "wind_speed", "wind_direction", "power_output"],
):
    """
    Filter columns from a Spark DataFrame.

    :param df: Spark DataFrame
    :param columns: Columns to keep
    :return: Spark DataFrame
    """
    return df.select(columns)
