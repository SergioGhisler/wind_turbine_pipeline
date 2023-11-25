import sys
import logging

import config
import data_ingestion
import data_cleaning
import summary_statistics
import anomaly_detection
import database
from aux_functions import clean_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Fetching latest timestamps from database...")
        latest_timestamps = database.get_latest_timestamps(
            jdbc_url=config.JDBC_URL, properties=config.DATABASE_PROPERTIES
        )

        logger.info("Starting data ingestion...")
        session = database.create_spark_session()
        raw_data = data_ingestion.read_data(
            spark=session,
            data_path=config.RAW_DATA_PATH,
            latest_timestamps=latest_timestamps,
        )

        if raw_data.count() == 0:
            logger.info("No new data to process. Exiting...")
            sys.exit(0)

        logger.info(f"Found {raw_data.count()} new rows of data to process.")

        logger.info("Performing data cleaning...")
        cleaned_data = data_cleaning.clean_data(raw_data)

        logger.info("Calculating summary statistics...")
        stats_df = summary_statistics.calculate_daily_summary_statistics(cleaned_data)

        logger.info("Detecting anomalies...")
        processed_data = anomaly_detection.detect_anomalies(cleaned_data, stats_df)

        logger.info("Storing summary statistics...")
        database.store_data_to_postgres(
            stats_df,
            "daily_summary_statistics",
            config.JDBC_URL,
            config.DATABASE_PROPERTIES,
        )

        logger.info("Storing processed data...")
        processed_data = clean_columns(processed_data, config.COLUMNS)

        database.store_data_to_postgres(
            processed_data,
            "turbine_data",
            config.JDBC_URL,
            config.DATABASE_PROPERTIES,
        )

        logger.info("Data processing pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
