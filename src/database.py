from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import config
import sys


def create_spark_session(
    app_name="wind_turbine_data_pipeline",
    timezone="UTC",
    postgres_driver_path=config.POSTGRES_DRIVER_PATH,
) -> SparkSession:
    """
    Creates a Spark session.

    :param app_name: Name of the Spark application.
    :param timezone: Timezone to be used by the Spark session.
    :return: Spark session object.
    """
    session = (
        SparkSession.builder.config("spark.jars", postgres_driver_path)
        .appName(app_name)
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", timezone)

    return session


def get_latest_timestamps(
    jdbc_url: str,
    properties: dict,
    table: str = "turbine_data",
    timestamp_col: str = "timestamp",
    group_col: str = "turbine_id",
) -> DataFrame:
    """
    Fetch the latest timestamps for each turbine from a PostgreSQL database.

    :param jdbc_url: JDBC URL of the PostgreSQL database.
    :param properties: Dictionary containing PostgreSQL properties.
    :param table: Name of the table to be queried.
    :param timestamp_col: Name of the timestamp column.
    :param group_col: Name of the column to be used for grouping.

    :return: DataFrame containing the latest timestamps for each turbine id.
    """
    try:
        spark = create_spark_session()
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
        df = df.orderBy(timestamp_col, ascending=False)
        df = (
            df.groupBy(group_col)
            .agg({timestamp_col: "first"})
            .withColumnRenamed(f"first({timestamp_col})", f"latest_{timestamp_col}")
        )
    except Exception as e:
        print("Error connecting to PostgreSQL database:", e)
        sys.exit(1)

    return df


def store_data_to_postgres(
    df: DataFrame,
    table_name: str,
    jdbc_url: str,
    properties: dict,
    mode: str = "append",
):
    """
    Store the DataFrame into a PostgreSQL table.

    :param df: DataFrame to be stored.
    :param table_name: Name of the table in PostgreSQL.
    :param jdbc_url: JDBC URL for the PostgreSQL database.
    :param properties: Dictionary containing PostgreSQL properties.
    """
    try:
        df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)
    except Exception as e:
        print("Error connecting to PostgreSQL database:", e)
        sys.exit(1)
