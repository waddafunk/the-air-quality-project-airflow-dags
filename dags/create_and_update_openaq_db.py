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
        # STEP 1: Create the tables dynamically based on CSV columns
        
        # For countries table
        countries_columns = list(countries_data.columns)
        countries_columns_sql = []
        
        # Ensure 'id' is the primary key
        countries_columns_sql.append("id VARCHAR(255) PRIMARY KEY")
        
        # Add remaining columns (excluding 'id' and execution_date which we already handled)
        for col in countries_columns:
            if col not in ['id', 'execution_date']:
                countries_columns_sql.append(f"{col} VARCHAR(255)")
        
        # Add execution_date column
        countries_columns_sql.append("execution_date DATE")
        countries_columns_sql.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        # Create countries table
        create_countries_table_query = f"""
        CREATE TABLE IF NOT EXISTS countries (
            {', '.join(countries_columns_sql)}
        );
        """
        cursor.execute(create_countries_table_query)
        
        # For locations table
        locations_columns = list(locations_data.columns)
        locations_columns_sql = []
        
        # Ensure 'id' is the primary key
        locations_columns_sql.append("id VARCHAR(255) PRIMARY KEY")
        
        # Add remaining columns with appropriate data types
        for col in locations_columns:
            if col == 'id' or col == 'execution_date':
                continue  # Already handled
            elif col in ['latitude', 'longitude']:
                locations_columns_sql.append(f"{col} FLOAT")
            elif col == 'country_id':
                locations_columns_sql.append(f"{col} VARCHAR(255) REFERENCES countries(id)")
            else:
                locations_columns_sql.append(f"{col} VARCHAR(255)")
        
        # Add execution_date column
        locations_columns_sql.append("execution_date DATE")
        locations_columns_sql.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        # Create locations table
        create_locations_table_query = f"""
        CREATE TABLE IF NOT EXISTS locations (
            {', '.join(locations_columns_sql)}
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
            
            # Prepare column names and values for this row
            columns = []
            placeholders = []
            values = []
            update_pairs = []
            
            for col in countries_data.columns:
                if not pd.isna(row[col]):  # Skip NULL values
                    columns.append(col)
                    placeholders.append('%s')
                    values.append(str(row[col]))
                    
                    if col != 'id':  # For UPDATE statement
                        update_pairs.append(f"{col} = %s")
            
            # Add execution_date if not already present
            if 'execution_date' not in columns:
                columns.append('execution_date')
                placeholders.append('%s')
                values.append(execution_date)
                update_pairs.append("execution_date = %s")
            
            # Check if country exists
            cursor.execute("SELECT 1 FROM countries WHERE id = %s", (country_id,))
            exists = cursor.fetchone()
            
            try:
                if exists:
                    # Update existing country
                    update_values = [val for i, val in enumerate(values) if columns[i] != 'id']
                    update_values.append(country_id)  # Add id for WHERE clause
                    
                    update_query = f"""
                    UPDATE countries 
                    SET {', '.join(update_pairs)}, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    cursor.execute(update_query, update_values)
                    countries_updated += 1
                else:
                    # Insert new country
                    insert_query = f"""
                    INSERT INTO countries ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    """
                    cursor.execute(insert_query, values)
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
        
        for _, row in locations_data.iterrows():
            # Skip if id is missing
            if pd.isna(row.get('id', None)):
                locations_skipped += 1
                continue
                
            location_id = str(row['id'])
            
            # Check for the country_id column
            country_id = None
            if 'country_id' in row and not pd.isna(row['country_id']):
                country_id = str(row['country_id'])
            
            # Skip if country_id is missing or not in the database
            if not country_id or country_id not in db_country_ids:
                print(f"Skipping location {location_id}: country_id {country_id} not found in countries table")
                locations_skipped += 1
                continue
            
            # Prepare column names and values for this row
            columns = []
            placeholders = []
            values = []
            update_pairs = []
            
            for col in locations_data.columns:
                if not pd.isna(row[col]):  # Skip NULL values
                    columns.append(col)
                    placeholders.append('%s')
                    values.append(str(row[col]))
                    
                    if col != 'id':  # For UPDATE statement
                        update_pairs.append(f"{col} = %s")
            
            # Add execution_date if not already present
            if 'execution_date' not in columns:
                columns.append('execution_date')
                placeholders.append('%s')
                values.append(execution_date)
                update_pairs.append("execution_date = %s")
            
            # Check if location exists
            cursor.execute("SELECT 1 FROM locations WHERE id = %s", (location_id,))
            exists = cursor.fetchone()
            
            try:
                if exists:
                    # Update existing location
                    update_values = [val for i, val in enumerate(values) if columns[i] != 'id']
                    update_values.append(location_id)  # Add id for WHERE clause
                    
                    update_query = f"""
                    UPDATE locations 
                    SET {', '.join(update_pairs)}, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    cursor.execute(update_query, update_values)
                    locations_updated += 1
                else:
                    # Insert new location
                    insert_query = f"""
                    INSERT INTO locations ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    """
                    cursor.execute(insert_query, values)
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
