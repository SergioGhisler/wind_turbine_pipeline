from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def detect_anomalies(
    df: DataFrame,
    stats_df,
    std_dev_factor: int = 2,
    date_col: str = "date",
    timestamp_col: str = "timestamp",
    group_col: str = "turbine_id",
    anomaly_col: str = "is_anomaly",
    target_col: str = "power_output",
) -> DataFrame:
    """
    Detect anomalies in a DataFrame using summary statistics.

    :param df: Input DataFrame containing turbine data.
    :param stats_df: DataFrame containing summary statistics.
    :param std_dev_factor: Number of standard deviations from the mean to use for anomaly detection.
    :return: DataFrame containing anomalies.
    """
    df_date_col = df.withColumn(date_col, F.to_date(timestamp_col))

    joined_df = df_date_col.join(stats_df, on=[group_col, date_col], how="left_outer")
    joined_df = joined_df.withColumn(
        anomaly_col,
        F.when(
            F.col(target_col)
            > F.col(f"avg_{target_col}")
            + std_dev_factor * F.col(f"stddev_{target_col}"),
            True,
        ).otherwise(False),
    )
    joined_df = joined_df.select(df.columns + [anomaly_col])
    joined_df = joined_df.sort(group_col, timestamp_col)
    return joined_df
