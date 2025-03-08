import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from utils.openaq import OpenAQv3Collector
import json

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
    """Upload openaq data to PostgreSQL database preserving all columns from the CSV files"""
    print("Starting upload_openaq_data function")
    # Retrieve file paths from previous task
    ti = context["ti"]
    countries_path, locations_path = ti.xcom_pull(task_ids="download_openaq_data")

    # Get execution date for data versioning
    execution_date = context["ds"]

    # Read the CSV files
    countries_data = pd.read_csv(countries_path)
    locations_data = pd.read_csv(locations_path)
    
    # Print column information for debugging
    print(f"Countries columns: {list(countries_data.columns)}")
    print(f"Locations columns: {list(locations_data.columns)}")
    
    # Clean up column names - replace spaces, colons and other problematic characters
    countries_data.columns = [col.replace(":", "_").replace(" ", "_").replace(".", "_").lower() 
                             for col in countries_data.columns]
    locations_data.columns = [col.replace(":", "_").replace(" ", "_").replace(".", "_").lower() 
                             for col in locations_data.columns]
    
    # Drop 'unnamed' columns if they exist
    countries_data = countries_data.loc[:, ~countries_data.columns.str.contains('^unnamed')]
    locations_data = locations_data.loc[:, ~locations_data.columns.str.contains('^unnamed')]
    
    # Print cleaned column names
    print(f"Cleaned countries columns: {list(countries_data.columns)}")
    print(f"Cleaned locations columns: {list(locations_data.columns)}")
    
    # Add execution date column for tracking when data was loaded
    countries_data["execution_date"] = execution_date
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
    conn.autocommit = False  # Ensure transactions are properly handled
    cursor = conn.cursor()

    try:
        # STEP 1: Create the tables with fixed schema based on our knowledge of the data
        
        # For countries table - create a simple schema
        create_countries_table_query = """
        CREATE TABLE IF NOT EXISTS countries (
            id VARCHAR(255) PRIMARY KEY,
            code VARCHAR(10),
            name VARCHAR(255),
            datetimefirst VARCHAR(255),
            datetimelast VARCHAR(255),
            parameters TEXT,
            execution_date DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_countries_table_query)
        
        # For locations table - identify the country ID column
        country_id_col = None
        for col in locations_data.columns:
            if 'country_id' in col:
                country_id_col = col
                break
        
        if not country_id_col:
            print("WARNING: Could not find country_id column in locations data")
            country_id_col = 'country_id'  # Default name if not found
        
        # Create locations table with a foreign key to countries
        create_locations_table_query = """
        CREATE TABLE IF NOT EXISTS locations (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            locality VARCHAR(255),
            timezone VARCHAR(50),
            ismobile BOOLEAN,
            ismonitor BOOLEAN,
            country_id VARCHAR(255) REFERENCES countries(id),
            latitude FLOAT,
            longitude FLOAT,
            execution_date DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metadata JSONB  -- Store additional fields as JSON
        );
        """
        cursor.execute(create_locations_table_query)
        
        # Commit table creation
        conn.commit()
        print("Tables created or verified successfully")
        
        # STEP 2: Upload countries data first
        print(f"Starting to process {len(countries_data)} countries")
        countries_inserted = 0
        countries_updated = 0
        
        for _, row in countries_data.iterrows():
            # Skip if id is missing
            if pd.isna(row.get('id', None)):
                continue
                
            country_id = str(row['id'])
            
            # Extract standard fields
            country_values = {
                'id': country_id,
                'code': str(row.get('code', '')) if not pd.isna(row.get('code', None)) else '',
                'name': str(row.get('name', '')) if not pd.isna(row.get('name', None)) else '',
                'datetimefirst': str(row.get('datetimefirst', '')) if not pd.isna(row.get('datetimefirst', None)) else '',
                'datetimelast': str(row.get('datetimelast', '')) if not pd.isna(row.get('datetimelast', None)) else '',
                'parameters': str(row.get('parameters', '')) if not pd.isna(row.get('parameters', None)) else '',
                'execution_date': execution_date
            }
            
            # Check if country exists
            cursor.execute("SELECT 1 FROM countries WHERE id = %s", (country_id,))
            exists = cursor.fetchone()
            
            try:
                if exists:
                    # Update existing country
                    update_query = """
                    UPDATE countries 
                    SET code = %s, name = %s, datetimefirst = %s, datetimelast = %s, 
                        parameters = %s, execution_date = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    cursor.execute(update_query, (
                        country_values['code'],
                        country_values['name'],
                        country_values['datetimefirst'],
                        country_values['datetimelast'],
                        country_values['parameters'],
                        country_values['execution_date'],
                        country_id
                    ))
                    countries_updated += 1
                else:
                    # Insert new country
                    insert_query = """
                    INSERT INTO countries (id, code, name, datetimefirst, datetimelast, parameters, execution_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        country_values['id'],
                        country_values['code'],
                        country_values['name'],
                        country_values['datetimefirst'],
                        country_values['datetimelast'],
                        country_values['parameters'],
                        country_values['execution_date']
                    ))
                    countries_inserted += 1
            except Exception as e:
                print(f"Error processing country {country_id}: {str(e)}")
                continue
        
        # Commit countries data
        conn.commit()
        print(f"Countries processed: {len(countries_data)}")
        print(f"Countries inserted: {countries_inserted}")
        print(f"Countries updated: {countries_updated}")
        
        # STEP 3: Upload locations data
        print(f"Starting to process {len(locations_data)} locations")
        locations_inserted = 0
        locations_updated = 0
        locations_skipped = 0
        
        # First, get all country IDs for foreign key validation
        cursor.execute("SELECT id FROM countries")
        db_country_ids = set(row[0] for row in cursor.fetchall())
        
        # Find the proper country and coordinate columns
        country_id_column = None
        lat_column = None
        lon_column = None
        
        for col in locations_data.columns:
            if 'country_id' in col.lower():
                country_id_column = col
            elif 'latitude' in col.lower() or 'coordinates_latitude' in col.lower():
                lat_column = col
            elif 'longitude' in col.lower() or 'coordinates_longitude' in col.lower():
                lon_column = col
        
        print(f"Using country_id column: {country_id_column}")
        print(f"Using latitude column: {lat_column}")
        print(f"Using longitude column: {lon_column}")
        
        for _, row in locations_data.iterrows():
            # Skip if id is missing
            if pd.isna(row.get('id', None)):
                locations_skipped += 1
                continue
                
            location_id = str(row['id'])
            
            # Extract country_id from the appropriate column
            country_id = None
            if country_id_column and country_id_column in row and not pd.isna(row[country_id_column]):
                country_id = str(row[country_id_column])
            
            # Skip if country_id is missing or not in the database
            if not country_id or country_id not in db_country_ids:
                print(f"Skipping location {location_id}: country_id {country_id} not found in countries table")
                locations_skipped += 1
                continue
                
            # Extract coordinates
            latitude = None
            longitude = None
            
            if lat_column and lat_column in row and not pd.isna(row[lat_column]):
                try:
                    latitude = float(row[lat_column])
                except (ValueError, TypeError):
                    pass
                    
            if lon_column and lon_column in row and not pd.isna(row[lon_column]):
                try:
                    longitude = float(row[lon_column])
                except (ValueError, TypeError):
                    pass
            
            # Store non-standard fields as JSON
            metadata = {}
            for col in locations_data.columns:
                if col not in ['id', 'name', 'locality', 'timezone', 'ismobile', 'ismonitor', 
                               country_id_column, lat_column, lon_column, 'execution_date'] and not pd.isna(row[col]):
                    metadata[col] = str(row[col])
            
            # Basic location fields
            location_values = {
                'id': location_id,
                'name': str(row.get('name', '')) if not pd.isna(row.get('name', None)) else '',
                'locality': str(row.get('locality', '')) if not pd.isna(row.get('locality', None)) else '',
                'timezone': str(row.get('timezone', '')) if not pd.isna(row.get('timezone', None)) else '',
                'ismobile': bool(row.get('ismobile', False)) if not pd.isna(row.get('ismobile', None)) else False,
                'ismonitor': bool(row.get('ismonitor', False)) if not pd.isna(row.get('ismonitor', None)) else False,
                'country_id': country_id,
                'latitude': latitude,
                'longitude': longitude,
                'execution_date': execution_date,
                'metadata': json.dumps(metadata)
            }
            
            # Check if location exists
            cursor.execute("SELECT 1 FROM locations WHERE id = %s", (location_id,))
            exists = cursor.fetchone()
            
            try:
                if exists:
                    # Update existing location
                    update_query = """
                    UPDATE locations 
                    SET name = %s, locality = %s, timezone = %s, ismobile = %s, ismonitor = %s,
                        country_id = %s, latitude = %s, longitude = %s, execution_date = %s,
                        metadata = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    cursor.execute(update_query, (
                        location_values['name'],
                        location_values['locality'],
                        location_values['timezone'],
                        location_values['ismobile'],
                        location_values['ismonitor'],
                        location_values['country_id'],
                        location_values['latitude'],
                        location_values['longitude'],
                        location_values['execution_date'],
                        location_values['metadata'],
                        location_id
                    ))
                    locations_updated += 1
                else:
                    # Insert new location
                    insert_query = """
                    INSERT INTO locations (id, name, locality, timezone, ismobile, ismonitor,
                                          country_id, latitude, longitude, execution_date, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        location_values['id'],
                        location_values['name'],
                        location_values['locality'],
                        location_values['timezone'],
                        location_values['ismobile'],
                        location_values['ismonitor'],
                        location_values['country_id'],
                        location_values['latitude'],
                        location_values['longitude'],
                        location_values['execution_date'],
                        location_values['metadata']
                    ))
                    locations_inserted += 1
                
                # Commit after each successful operation
                conn.commit()
                
            except Exception as e:
                conn.rollback()
                print(f"Error processing location {location_id}: {str(e)}")
                locations_skipped += 1
                continue
        
        # Verify final counts
        cursor.execute("SELECT COUNT(*) FROM countries")
        countries_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM locations")
        locations_count = cursor.fetchone()[0]
        
        print(f"Locations processed: {len(locations_data)}")
        print(f"Locations inserted: {locations_inserted}")
        print(f"Locations updated: {locations_updated}")
        print(f"Locations skipped: {locations_skipped}")
        print(f"Total countries in database: {countries_count}")
        print(f"Total locations in database: {locations_count}")
        
    except Exception as e:
        conn.rollback()
        print(f"Critical error in upload_openaq_data: {str(e)}")
        raise e
    finally:
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
