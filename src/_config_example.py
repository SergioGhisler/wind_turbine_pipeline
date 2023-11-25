RAW_DATA_PATH = "../data/raw_data"

POSTGRES_DRIVER_PATH = "../drivers/postgresql-42.7.0.jar"

JDBC_URL = "jdbc:postgresql://localhost:5432/wind_turbine_data"

DATABASE_PROPERTIES = {
    "user": "your_username", #Change this to your username
    "password": "your_password", #Change this to your password
    "driver": "org.postgresql.Driver",
}

COLUMNS = [
    "timestamp",
    "turbine_id",
    "wind_speed",
    "wind_direction",
    "power_output",
]
