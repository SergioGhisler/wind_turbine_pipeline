import pytest
from pyspark.sql import SparkSession
import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from anomaly_detection import detect_anomalies


def test_detect_anomalies(spark_session):
    schema = "turbine_id string, timestamp timestamp, power_output double"
    data = [
        (1, datetime.datetime(2022, 1, 1, 0, 0), 100.0),
        (1, datetime.datetime(2022, 1, 1, 1, 0), 900.0),
        (1, datetime.datetime(2022, 1, 2, 0, 0), 200.0),
        (2, datetime.datetime(2022, 1, 1, 0, 0), 450.0),
        (2, datetime.datetime(2022, 1, 1, 1, 0), 160.0),
        (2, datetime.datetime(2022, 1, 1, 2, 0), 200.0),
    ]
    df = spark_session.createDataFrame(data, schema)

    stats_data = [
        (1, datetime.date(2022, 1, 1), 100.0, 150.0, 125.0, 35.35533905932738),
        (1, datetime.date(2022, 1, 2), 200.0, 200.0, 200.0, None),
        (2, datetime.date(2022, 1, 1), 120.0, 200.0, 160.0, 40.0),
    ]
    stats_schema = "turbine_id string, date date, min_power_output double, max_power_output double, avg_power_output double, stddev_power_output double"
    stats_df = spark_session.createDataFrame(stats_data, stats_schema)

    result_df = detect_anomalies(df, stats_df)

    expected_data = [
        (1, datetime.datetime(2022, 1, 1, 0, 0), 100.0, False),
        (1, datetime.datetime(2022, 1, 1, 1, 0), 900.0, True),
        (1, datetime.datetime(2022, 1, 2, 0, 0), 200.0, False),
        (2, datetime.datetime(2022, 1, 1, 0, 0), 450.0, True),
        (2, datetime.datetime(2022, 1, 1, 1, 0), 160.0, False),
        (2, datetime.datetime(2022, 1, 1, 2, 0), 200.0, False),
    ]
    expected_columns = "turbine_id string, timestamp timestamp, power_output double, is_anomaly boolean"
    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    assert result_df.collect() == expected_df.collect()


@pytest.fixture(scope="module")
def spark_session():
    session = SparkSession.builder.appName("pytest").getOrCreate()
    yield session
    session.stop()
