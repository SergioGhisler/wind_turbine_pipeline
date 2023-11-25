CREATE DATABASE wind_turbine_data;

\c wind_turbine_data;

CREATE TABLE IF NOT EXISTS turbine_data (
    turbine_id INT,
    timestamp TIMESTAMP,
    wind_speed DECIMAL(10, 2),
    wind_direction DECIMAL(10, 2),
    power_output DECIMAL(10, 2),
    is_anomaly BOOLEAN,
    PRIMARY KEY (turbine_id, timestamp)
);

CREATE TABLE IF NOT EXISTS daily_summary_statistics (
    turbine_id INT,
    date DATE,
    min_power_output DECIMAL(10, 2),
    max_power_output DECIMAL(10, 2),
    avg_power_output DECIMAL(10, 2),
    stddev_power_output DECIMAL(10, 2),
    PRIMARY KEY (turbine_id, date)
);