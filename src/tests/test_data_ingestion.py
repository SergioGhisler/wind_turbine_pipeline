import pytest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pyspark.sql import SparkSession
from data_ingestion import read_data
import datetime
from pyspark.sql.functions import col


def test_read_data(spark_session):
    """
    Test function to check if read_data function reads data correctly.
    It also checks if the function filters out rows with timestamps that are less than or equal to the latest timestamp.

    :param spark_session: Spark session object.
    """
    mock_latest_timestamps = [
        (1, datetime.datetime(2022, 3, 4, 0, 0)),
    ]
    mock_schema = "turbine_id string, latest_timestamp timestamp"
    mock_latest_timestamps = spark_session.createDataFrame(
        mock_latest_timestamps, mock_schema
    )

    test_data_path = "data/test_data/"
    df = read_data(spark_session, test_data_path, mock_latest_timestamps)
    max_timestamp = datetime.datetime(2022, 3, 4, 0, 0)
    count_filtered_df_1 = df.filter(
        (col("turbine_id") == 1) & (col("timestamp") <= max_timestamp)
    ).count()
    count_filtered_df_2 = df.filter(
        (col("turbine_id") == 2) & (col("timestamp") <= max_timestamp)
    ).count()

    assert df.count() > 0
    assert count_filtered_df_1 == 0
    assert count_filtered_df_2 != 0


@pytest.fixture(scope="module")
def spark_session():
    session = SparkSession.builder.appName("pytest").getOrCreate()
    yield session
    session.stop()
