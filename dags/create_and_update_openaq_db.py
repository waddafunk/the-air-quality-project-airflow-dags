import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from utils.openaq import OpenAQv3Collector

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

    # Print column names to debug
    print(f"Countries columns: {list(countries_data.columns)}")
    print(f"Locations columns: {list(locations_data.columns)}")
    
    # Sample data to verify structure
    print("Sample countries data:")
    print(countries_data.head(2).to_string())
    print("Sample locations data:")
    print(locations_data.head(2).to_string())

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

    # Check what country IDs exist in the database for verification
    cursor.execute("SELECT id FROM countries")
    db_country_ids = [row[0] for row in cursor.fetchall()]
    print(f"Number of countries in database: {len(db_country_ids)}")
    print(f"First few country IDs in database: {db_country_ids[:5]}")

    # Process locations data
    locations_inserted = 0
    locations_updated = 0
    locations_skipped = 0
    foreign_key_errors = 0

    for _, row in locations_data.iterrows():
        try:
            # Prepare data for insertion, handling potential missing columns
            location_id = str(row.get("id", ""))
            location_name = str(row.get("name", ""))
            city = str(row.get("city", ""))
            
            # Check which column contains the country ID
            if "country_id" in row:
                country_id = str(row.get("country_id", ""))
            elif "countryId" in row:
                country_id = str(row.get("countryId", ""))
            else:
                # Try to find any column that might contain the country ID
                possible_country_id_columns = [col for col in row.index if "country" in col.lower()]
                if possible_country_id_columns:
                    country_id = str(row.get(possible_country_id_columns[0], ""))
                else:
                    print(f"Could not find country ID column for location {location_id}")
                    locations_skipped += 1
                    continue

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
                locations_skipped += 1
                continue

            # Verify the country ID exists in the database
            if country_id not in db_country_ids:
                print(f"Country ID {country_id} for location {location_id} not found in countries table")
                foreign_key_errors += 1
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
                locations_updated += 1
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
                locations_inserted += 1

            # Commit after each successful operation to avoid losing all data on a single error
            conn.commit()
            
        except Exception as e:
            print(f"Error processing location {row.get('id', 'unknown')}: {str(e)}")
            conn.rollback()
            locations_skipped += 1

    # Verify locations were inserted
    cursor.execute("SELECT COUNT(*) FROM locations")
    locations_count = cursor.fetchone()[0]
    
    print(f"Locations processed: {len(locations_data)}")
    print(f"Locations inserted: {locations_inserted}")
    print(f"Locations updated: {locations_updated}")
    print(f"Locations skipped: {locations_skipped}")
    print(f"Foreign key errors: {foreign_key_errors}")
    print(f"Total locations in database: {locations_count}")

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
