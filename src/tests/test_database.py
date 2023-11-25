import pytest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import datetime
from pyspark.sql import SparkSession
from database import (
    get_latest_timestamps,
    create_spark_session,
)


@pytest.fixture(scope="module")
def spark_session():
    session = SparkSession.builder.appName("pytest").getOrCreate()
    yield session
    session.stop()


def test_create_spark_session():
    """
    Test function to check if create_spark_session function creates a Spark session with UTC timezone.
    """
    session = create_spark_session("test_app", "UTC")
    assert isinstance(session, SparkSession)
    assert session.conf.get("spark.sql.session.timeZone") == "UTC"


def mock_jdbc(*args, **kwargs):
    """
    Mock function to return a DataFrame with mock data
    """
    spark = SparkSession.builder.getOrCreate()

    data = [
        (1, datetime.datetime(2021, 1, 1, 0, 0)),
        (1, datetime.datetime(2021, 3, 1, 0, 0)),
        (2, datetime.datetime(2021, 1, 1, 1, 0)),
        (2, datetime.datetime(2021, 4, 1, 1, 0)),
    ]
    schema = "turbine_id string, timestamp timestamp"
    return spark.createDataFrame(data, schema)


def test_get_latest_timestamps(monkeypatch, spark_session):
    """
    Test function to check if get_latest_timestamps function returns the latest timestamps for each turbine id.

    :param monkeypatch: Pytest fixture to patch functions.
    :param spark_session: Spark session object.
    """
    monkeypatch.setattr("pyspark.sql.DataFrameReader.jdbc", mock_jdbc)

    result_df = get_latest_timestamps(
        jdbc_url="jdbc:postgresql://mockurl:5432/mockdb",
        properties={"user": "test", "password": "test"},
        table="test",
        timestamp_col="timestamp",
        group_col="turbine_id",
    )

    desired_output = [
        (1, datetime.datetime(2021, 3, 1, 0, 0)),
        (2, datetime.datetime(2021, 4, 1, 1, 0)),
    ]
    desired_schema = "turbine_id string, latest_timestamp timestamp"
    desired_df = spark_session.createDataFrame(desired_output, desired_schema)

    assert result_df.columns == ["turbine_id", "latest_timestamp"]
    assert result_df.count() == 2
    assert result_df.collect() == desired_df.collect()
