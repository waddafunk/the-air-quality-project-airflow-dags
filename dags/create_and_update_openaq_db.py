import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import requests
from openaq import OpenAQv3Collector

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_db_if_not_exists():
    """Create the 'openaq' database if it doesn't exist"""
    # Get PostgreSQL connection details from environment variables
    pg_host = os.environ.get("POSTGRES_HOST")
    pg_user = os.environ.get("POSTGRES_USER")
    pg_password = os.environ.get("POSTGRES_PASSWORD")

    # Connect to the default 'postgres' database first
    conn = psycopg2.connect(
        host=pg_host,
        user=pg_user,
        password=pg_password,
        dbname="postgres",
        port=os.environ.get("POSTGRES_PORT"),
    )

    # We need to set autocommit to create a database
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = conn.cursor()

    # Check if the database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='openaq'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("CREATE DATABASE openaq")
        print("Database 'openaq' created successfully")
    else:
        print("Database 'openaq' already exists")

    cursor.close()
    conn.close()


# Define the function to download openaq data
def download_openaq_data(**context):
    """Download openaq data from the specified URL"""

    # Get the URL from Airflow variables
    client = OpenAQv3Collector()

    countries = client.get_all_countries()
    countries_path = f"/tmp/openaq_data_{context['ds']}_countries.csv"
    countries.to_csv(countries_path)

    locations = client.get_locations(countries["id"].to_list(), 10000)
    locations_path = f"/tmp/openaq_data_{context['ds']}_locations.csv"  # Fixed file name to 'locations'
    locations.to_csv(locations_path)

    return countries_path, locations_path


def upload_openaq_data(**context):
    """Upload openaq data to PostgreSQL database by country with incremental updates"""
    print("Starting upload_openaq_data function")
    # Retrieve file path from previous task
    ti = context["ti"]
    countries_path, locations_path = ti.xcom_pull(task_ids="download_openaq_data")

    # Get execution date for data versioning
    execution_date = context["ds"]

    # Read the CSV files
    countries_data = pd.read_csv(countries_path)
    locations_data = pd.read_csv(locations_path)

    # Ensure we have a date column for tracking updates
    if "execution_date" not in countries_data.columns:
        countries_data["execution_date"] = execution_date

    if "execution_date" not in locations_data.columns:
        locations_data["execution_date"] = execution_date

    # Get PostgreSQL connection details from environment variables
    pg_host = os.environ.get("POSTGRES_HOST")
    pg_user = os.environ.get("POSTGRES_USER")
    pg_password = os.environ.get("POSTGRES_PASSWORD")
    pg_port = os.environ.get("POSTGRES_PORT")

    # Connect to the openaq database
    conn = psycopg2.connect(
        host=pg_host, user=pg_user, password=pg_password, dbname="openaq", port=pg_port
    )

    cursor = conn.cursor()

    # Create countries table if it doesn't exist
    create_countries_table_query = """
    CREATE TABLE IF NOT EXISTS countries (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        code VARCHAR(10),
        execution_date DATE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_countries_table_query)

    # Create locations table if it doesn't exist with foreign key reference to countries
    create_locations_table_query = """
    CREATE TABLE IF NOT EXISTS locations (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        city VARCHAR(255),
        country_id VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT,
        execution_date DATE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES countries(id)
    );
    """
    cursor.execute(create_locations_table_query)

    # Commit the table creation
    conn.commit()

    # Process countries data
    for _, row in countries_data.iterrows():
        # Prepare data for insertion, handling potential missing columns
        country_id = str(row.get("id", ""))
        country_name = str(row.get("name", ""))
        country_code = str(row.get("code", ""))

        # Skip if any of the required fields are missing
        if not country_id:
            continue

        # Check if this country already exists
        cursor.execute("SELECT 1 FROM countries WHERE id = %s", (country_id,))
        exists = cursor.fetchone()

        if exists:
            # Update existing record
            update_query = """
            UPDATE countries 
            SET name = %s, code = %s, execution_date = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """
            cursor.execute(
                update_query, (country_name, country_code, execution_date, country_id)
            )
        else:
            # Insert new record
            insert_query = """
            INSERT INTO countries (id, name, code, execution_date)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(
                insert_query, (country_id, country_name, country_code, execution_date)
            )

    # Commit countries data
    conn.commit()
    print(f"Processed {len(countries_data)} countries")

    # Process locations data
    for _, row in locations_data.iterrows():
        # Prepare data for insertion, handling potential missing columns
        location_id = str(row.get("id", ""))
        location_name = str(row.get("name", ""))
        city = str(row.get("city", ""))
        country_id = str(row.get("country_id", ""))

        # Try to get latitude and longitude, defaulting to None if not available
        try:
            latitude = float(row.get("latitude", None))
        except (ValueError, TypeError):
            latitude = None

        try:
            longitude = float(row.get("longitude", None))
        except (ValueError, TypeError):
            longitude = None

        # Skip if any of the required fields are missing
        if not location_id or not country_id:
            continue

        # Check if this location already exists
        cursor.execute("SELECT 1 FROM locations WHERE id = %s", (location_id,))
        exists = cursor.fetchone()

        if exists:
            # Update existing record
            update_query = """
            UPDATE locations 
            SET name = %s, city = %s, country_id = %s, latitude = %s, longitude = %s, 
                execution_date = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """
            cursor.execute(
                update_query,
                (
                    location_name,
                    city,
                    country_id,
                    latitude,
                    longitude,
                    execution_date,
                    location_id,
                ),
            )
        else:
            # Insert new record
            insert_query = """
            INSERT INTO locations (id, name, city, country_id, latitude, longitude, execution_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(
                insert_query,
                (
                    location_id,
                    location_name,
                    city,
                    country_id,
                    latitude,
                    longitude,
                    execution_date,
                ),
            )

    # Commit locations data
    conn.commit()
    print(f"Processed {len(locations_data)} locations")

    # Close cursor and connection
    cursor.close()
    conn.close()

    print("Upload to database completed successfully")


# Define the DAG
with DAG(
    "create_and_update_openaq_database",
    default_args=default_args,
    description="Creates the openaq database if it does not exist and updates its tables",
    schedule=timedelta(days=3),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["openaq", "etl", "data-warehouse"],
) as dag:

    # Task to create the database
    create_database_task = PythonOperator(
        task_id="create_database_if_not_exists",
        python_callable=create_db_if_not_exists,
    )

    download_task = PythonOperator(
        task_id="download_openaq_data",
        python_callable=download_openaq_data,
    )

    upload_task = PythonOperator(
        task_id="upload_openaq_data",
        python_callable=upload_openaq_data,
    )

    # Set task dependencies
    # upload_task should start when both create_database_task and download_task are completed
    [create_database_task, download_task] >> upload_task

if __name__ == "__main__":
    dag.test()
