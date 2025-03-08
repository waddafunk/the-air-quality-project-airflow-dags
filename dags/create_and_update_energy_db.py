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
    """Upload energy data to PostgreSQL database by country with incremental updates"""
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
        
        # If table doesn't exist, create it
        if not table_exists:
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
        else:
            # Get existing columns in all cases (whether table existed or was just created)
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            existing_columns = [row[0] for row in cursor.fetchall()]
        
        if table_exists:
            # Table exists, check for any new columns in the dataframe
            for col_name, dtype in zip(df.columns, df.dtypes):
                if col_name.lower() not in [col.lower() for col in existing_columns]:
                    # Map pandas dtypes to PostgreSQL types
                    if 'int' in str(dtype):
                        pg_type = 'INTEGER'
                    elif 'float' in str(dtype):
                        pg_type = 'FLOAT'
                    elif 'datetime' in str(dtype):
                        pg_type = 'TIMESTAMP'
                    else:
                        pg_type = 'TEXT'
                    
                    # Add the new column
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{col_name}\" {pg_type}")
                    conn.commit()
                    print(f"Added new column '{col_name}' to table '{table_name}'")
                    
            # Refresh existing columns list after potential changes
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            existing_columns = [row[0] for row in cursor.fetchall()]
        
        # Determine the unique identifier for this data
        # This assumes your energy data has some form of timestamp or period identifier
        id_column = next((col for col in df.columns if col.lower() in ['id', 'date', 'timestamp', 'period', 'time']), None)
        
        try:
            if id_column:
                # For each row, try to update existing records, or insert new ones
                for _, row in df.iterrows():
                    try:
                        # Check if this record already exists
                        id_value = row[id_column]
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE \"{id_column}\" = %s", (id_value,))
                        exists = cursor.fetchone()[0] > 0
                        
                        if exists:
                            # Update existing record
                            set_clauses = []
                            values = []
                            
                            for col, val in row.items():
                                if col != id_column and col in existing_columns:
                                    set_clauses.append(f"\"{col}\" = %s")
                                    values.append(val)
                            
                            # Only proceed if there are columns to update
                            if set_clauses:
                                # Add updated_at timestamp if the column exists
                                if "updated_at" in existing_columns:
                                    set_clauses.append("updated_at = CURRENT_TIMESTAMP")
                                
                                # Add the id value for the WHERE clause
                                values.append(id_value)
                                
                                # Execute update
                                update_query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE \"{id_column}\" = %s"
                                cursor.execute(update_query, values)
                        else:
                            # Insert new record
                            columns = [col for col in row.index if col.lower() in [col.lower() for col in existing_columns]]
                            if columns:
                                placeholders = ', '.join(['%s'] * len(columns))
                                values = [row[col] for col in columns]
                                
                                # Insert data
                                columns_str = ', '.join([f'"{col}"' for col in columns])
                                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                                cursor.execute(insert_query, values)
                    except Exception as row_error:
                        print(f"Warning: Error processing row for {id_column}={row.get(id_column, 'unknown')}: {row_error}")
                        # Continue processing other rows
            else:
                # No suitable ID column found - we'll treat all data as new records
                print(f"Warning: No suitable ID column found for incremental updates in '{table_name}'. Adding all as new records.")
                
                # Generate values part of the SQL query
                for _, row in df.iterrows():
                    try:
                        columns = [col for col in row.index if col.lower() in [col.lower() for col in existing_columns]]
                        if columns:
                            placeholders = ', '.join(['%s'] * len(columns))
                            values = [row[col] for col in columns]
                            
                            # Insert data
                            columns_str = ', '.join([f'"{col}"' for col in columns])
                            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                            cursor.execute(insert_query, values)
                    except Exception as row_error:
                        print(f"Warning: Error inserting row: {row_error}")
                        # Continue processing other rows
        except Exception as e:
            print(f"Error processing data for table {table_name}: {e}")
            # Continue processing other country groups
        
        conn.commit()
        print(f"Incremental update for country '{country}' completed to table '{table_name}'")
    
    cursor.close()
    conn.close()
    
    print("All country data incrementally updated successfully")


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
