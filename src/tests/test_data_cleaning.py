import pytest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_cleaning import fill_missing_in_groups, remove_outliers
from pyspark.sql import SparkSession


def test_add_null_group_columns(spark_session):
    data = [
        (1, 1, 10),
        (1, 2, None),
        (1, 3, None),
        (1, 4, 40),
        (1, 5, 40),
        (1, 6, None),
        (1, 7, 20),
        (1, 8, None),
        (2, 0, None),
        (2, 1, 50),
        (2, 2, None),
        (2, 3, 70),
        (2, 4, None),
    ]
    columns = ["group_col", "order_col", "fill_col"]
    df = spark_session.createDataFrame(data, columns)

    result_df = fill_missing_in_groups(df, "group_col", "order_col", "fill_col")

    expected_data = [
        (1, 1, 10),
        (1, 2, 20),
        (1, 3, 30),
        (1, 4, 40),
        (1, 5, 40),
        (1, 6, 30),
        (1, 7, 20),
        (1, 8, 20),
        (2, 0, 50),
        (2, 1, 50),
        (2, 2, 60),
        (2, 3, 70),
        (2, 4, 70),
    ]
    expected_columns = [
        "group_col",
        "order_col",
        "fill_col",
    ]
    result_df = result_df.select(expected_columns)
    print(result_df.show())
    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    assert result_df.collect() == expected_df.collect()


def test_remove_outliers(spark_session):
    data = [
        (1, 10, 100),
        (2, 20, 200),
        (3, 30, 300),
        (4, 40, 400),
        (5, 50, 500),
        (6, 60, 600),
        (7, 70, 700),
        (8, 80, 800),
        (9, 90, 9000),
        (10, 100, 10000),
    ]
    columns = ["group_col", "order_col", "outlier_col"]
    df = spark_session.createDataFrame(data, columns)

    result_df = remove_outliers(df, columns_to_clean=["outlier_col"])

    expected_data = [
        (1, 10, 100),
        (2, 20, 200),
        (3, 30, 300),
        (4, 40, 400),
        (5, 50, 500),
        (6, 60, 600),
        (7, 70, 700),
        (8, 80, 800),
        (9, 90, None),
        (10, 100, None),
    ]
    expected_df = spark_session.createDataFrame(expected_data, columns)

    assert result_df.collect() == expected_df.collect()


@pytest.fixture(scope="module")
def spark_session():
    session = SparkSession.builder.appName("pytest").getOrCreate()
    yield session
    session.stop()
