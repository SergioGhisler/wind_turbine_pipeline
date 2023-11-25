from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from functools import reduce
import database
import config


def read_data(
    spark: SparkSession,
    data_path: str,
    latest_timestamps: DataFrame,
    infer_schema: bool = True,
    header: bool = True,
) -> DataFrame:
    """
    Read data from a directory containing CSV files, and merge them into a single DataFrame.
    Filter out rows with timestamps that are less than or equal to the latest timestamp for each turbine id.

    :param spark: Spark session object.
    :param data_path: Directory containing CSV files.
    :param latest_timestamps: DataFrame containing the latest timestamps for each turbine id.
    :param infer_schema: Whether to automatically infer the schema of the CSV files.
    :param header: Whether the CSV files contain a header.

    :return: Spark DataFrame containing all data from the CSV files.
    """

    df = spark.read.csv(
        data_path,
        inferSchema=infer_schema,
        header=header,
    )

    joined_df = df.join(latest_timestamps, on=["turbine_id"], how="left_outer")

    filtered_df = joined_df.filter(
        (col("timestamp") > col("latest_timestamp")) | col("latest_timestamp").isNull()
    )

    filtered_df = filtered_df.select(df.columns)
    return filtered_df


def main():
    postgres_driver_path = "drivers/postgresql-42.7.0.jar"
    spark_session = database.create_spark_session(
        postgres_driver_path=postgres_driver_path
    )
    data_dir = "data/test_data/"
    latest_timestamps = database.get_latest_timestamps(
        jdbc_url=config.JDBC_URL, properties=config.DATABASE_PROPERTIES
    )
    print(latest_timestamps.show())
    df = read_data(spark_session, data_dir, latest_timestamps=latest_timestamps)
    return df


if __name__ == "__main__":
    df = main()
    df.show()
