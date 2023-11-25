import pytest
from pyspark.sql import SparkSession
import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from summary_statistics import calculate_daily_summary_statistics


def test_calculate_daily_summary_statistics(spark_session):
    schema = "turbine_id string, timestamp timestamp, power_output double"
    data = [
        (1, datetime.datetime(2022, 1, 1, 0, 0), 100.0),
        (1, datetime.datetime(2022, 1, 1, 1, 0), 150.0),
        (1, datetime.datetime(2022, 1, 2, 0, 0), 200.0),
        (2, datetime.datetime(2022, 1, 1, 0, 0), 120.0),
        (2, datetime.datetime(2022, 1, 1, 1, 0), 160.0),
        (2, datetime.datetime(2022, 1, 1, 0, 0), 200.0),
    ]
    df = spark_session.createDataFrame(data, schema)

    result_df = calculate_daily_summary_statistics(df, "power_output")

    expected_data = [
        (1, datetime.date(2022, 1, 1), 100.0, 150.0, 125.0, 35.35533905932738),
        (1, datetime.date(2022, 1, 2), 200.0, 200.0, 200.0, None),
        (2, datetime.date(2022, 1, 1), 120.0, 200.0, 160.0, 40.0),
    ]
    expected_schema = "turbine_id string, date date, min_power_output double, max_power_output double, avg_power_output double, stddev_power_output double"

    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    assert result_df.collect() == expected_df.collect()


@pytest.fixture(scope="module")
def spark_session():
    session = SparkSession.builder.appName("pytest").getOrCreate()
    yield session
    session.stop()
