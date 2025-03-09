import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_db_if_not_exists():
    """Create the 'energy' database if it doesn't exist"""
    # Get PostgreSQL connection details from environment variables
    pg_host = os.environ.get('POSTGRES_HOST')
    pg_user = os.environ.get('POSTGRES_USER')
    pg_password = os.environ.get('POSTGRES_PASSWORD')
    
    # Connect to the default 'postgres' database first
    conn = psycopg2.connect(
        host=pg_host,
        user=pg_user,
        password=pg_password,
        dbname='postgres',
        port=os.environ.get("POSTGRES_PORT")
    )
    
    # We need to set autocommit to create a database
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    cursor = conn.cursor()
    
    # Check if the database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='energy'")
    exists = cursor.fetchone()
    
    if not exists:
        cursor.execute("CREATE DATABASE energy")
        print("Database 'energy' created successfully")
    else:
        print("Database 'energy' already exists")
    
    cursor.close()
    conn.close()

# Define the function to download energy data
def download_energy_data(**context):
    """Download energy data from the specified URL"""
    
    # Get the URL from Airflow variables
    energy_data_url = os.environ["ENERGY_DATA_URL"]
    
    # Create the file path using the execution date
    file_path = f"/tmp/energy_data_{context['ds']}.csv"
    
    # Download the file
    response = requests.get(energy_data_url, stream=True)
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    # Write the content to a file
    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Successfully downloaded energy data to {file_path}")
    return file_path

def upload_energy_data(**context):
    """Upload energy data to PostgreSQL database by country with simple replace logic"""
    print("Starting upload_energy_data function")
    # Retrieve file path from previous task
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='download_energy_data')
    
    # Get execution date for data versioning
    execution_date = context['ds']
    
    # Read the CSV file
    energy_data = pd.read_csv(file_path)
    
    # Ensure we have a date column for tracking updates
    # If the data doesn't have a date column already, add the execution date
    if 'date' not in energy_data.columns:
        energy_data['date'] = execution_date
    
    # Get PostgreSQL connection details from environment variables
    pg_host = os.environ.get('POSTGRES_HOST')
    pg_user = os.environ.get('POSTGRES_USER')
    pg_password = os.environ.get('POSTGRES_PASSWORD')
    pg_port = os.environ.get('POSTGRES_PORT')
    
    # Connect to the energy database
    conn = psycopg2.connect(
        host=pg_host,
        user=pg_user,
        password=pg_password,
        dbname='energy',
        port=pg_port
    )
    
    cursor = conn.cursor()
    
    # Process data by country
    for country, df in energy_data.groupby("country"):
        # Sanitize the country name for table naming - handle special characters
        # Replace any non-alphanumeric characters with underscores
        import re
        sanitized_country = re.sub(r'[^\w]', '_', country.lower())
        table_name = f"energy_{sanitized_country}"
        
        # Check if table exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        # If table exists, drop it
        if table_exists:
            cursor.execute(f"DROP TABLE {table_name}")
            conn.commit()
            print(f"Dropped existing table '{table_name}'")
        
        # Create table with the new data
        # Get column names and types from DataFrame
        columns = []
        for col_name, dtype in zip(df.columns, df.dtypes):
            # Map pandas dtypes to PostgreSQL types
            if 'int' in str(dtype):
                pg_type = 'INTEGER'
            elif 'float' in str(dtype):
                pg_type = 'FLOAT'
            elif 'datetime' in str(dtype):
                pg_type = 'TIMESTAMP'
            else:
                pg_type = 'TEXT'
            columns.append(f"\"{col_name}\" {pg_type}")
        
        # Add a unique identifier column for updates if not already present
        if 'id' not in df.columns:
            columns.append("id SERIAL PRIMARY KEY")
        
        # Add version tracking columns if not already present
        if 'created_at' not in df.columns:
            columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if 'updated_at' not in df.columns:
            columns.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        # Create table
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        cursor.execute(create_table_query)
        
        try:
            # Create indexes for efficient lookups
            # Assume the first column that's not country is the primary data identifier (like time or date)
            identifier_col = next((col for col in df.columns if col != 'country'), 'date')
            cursor.execute(f"CREATE INDEX idx_{table_name}_{identifier_col} ON {table_name} (\"{identifier_col}\")")
        except Exception as e:
            print(f"Warning: Could not create index: {e}")
        
        conn.commit()
        print(f"Created new table '{table_name}' with indexes")
        
        # Insert all data into the table
        try:
            # Get the columns in the table
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            existing_columns = [row[0] for row in cursor.fetchall()]
            
            # Filter columns that exist in the table
            valid_columns = [col for col in df.columns if col.lower() in [col.lower() for col in existing_columns]]
            
            if valid_columns:
                # Insert data in bulk
                print(f"Inserting {len(df)} rows into {table_name}")
                
                # Prepare values list for all rows
                values_list = []
                for _, row in df.iterrows():
                    row_values = [row[col] for col in valid_columns]
                    values_list.append(row_values)
                
                # Create the SQL for bulk insert
                placeholders = ', '.join(['%s'] * len(valid_columns))
                columns_str = ', '.join([f'"{col}"' for col in valid_columns])
                
                # Use executemany for efficient bulk insert
                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                cursor.executemany(insert_query, values_list)
                conn.commit()
                
        except Exception as e:
            print(f"Error inserting data into table {table_name}: {e}")
            # Continue processing other country groups
        
        print(f"Data loaded for country '{country}' into table '{table_name}'")
    
    cursor.close()
    conn.close()
    
    print("All country data uploaded successfully")


# Define the DAG
with DAG(
    'create_and_update_energy_database',
    default_args=default_args,
    description='Creates the energy database if it does not exist and updates its tables',
    schedule=timedelta(days=3*31), 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["energy", "etl", "data-warehouse"]
) as dag:
    
    # Task to create the database
    create_database_task = PythonOperator(
        task_id='create_database_if_not_exists',
        python_callable=create_db_if_not_exists,
    )
    
    download_task = PythonOperator(
        task_id="download_energy_data",
        python_callable=download_energy_data,
    )
    
    upload_task = PythonOperator(
        task_id="upload_energy_data",
        python_callable=upload_energy_data,
    )
    
    # Set task dependencies
    # upload_task should start when both create_database_task and download_task are completed
    [create_database_task, download_task] >> upload_task

if __name__ == "__main__":
    dag.test()
