from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import data_cleaning


def calculate_daily_summary_statistics(
    df: DataFrame, power_output_col: str = "power_output"
) -> DataFrame:
    """
    Calculate summary statistics for each turbine daily.

    :param df: Input DataFrame containing turbine data.
    :param interval_hours: Time interval in hours for calculating statistics.
    :return: DataFrame with summary statistics.
    """

    df = df.withColumn("date", F.to_date("timestamp"))
    stats_df = df.groupBy("turbine_id", "date").agg(
        F.min(power_output_col).alias(f"min_{power_output_col}"),
        F.max(power_output_col).alias(f"max_{power_output_col}"),
        F.avg(power_output_col).alias(f"avg_{power_output_col}"),
        F.stddev(power_output_col).alias(f"stddev_{power_output_col}"),
    )
    stats_df = stats_df.sort("turbine_id", "date")
    return stats_df
