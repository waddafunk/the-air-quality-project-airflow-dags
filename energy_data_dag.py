from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "energy_data_pipeline",
    default_args=default_args,
    schedule_interval="@monthly",
)


# Define the function to upload to Azure Data Lake
def upload_to_datalake(ti, **kwargs):
    # Get the downloaded file path from XCom
    downloaded_file_path = ti.xcom_pull(task_ids=["download_energy_data"])[0]

    # Read the CSV file
    energy_df = pd.read_csv(downloaded_file_path)
    print(f"Read {len(energy_df)} rows from energy data file")

    # Get environment variables
    prefix = os.environ["PREFIX"].replace("-", "")

    # Storage account parameters
    STORAGE_ACCOUNT_NAME = prefix + "dlsdev"
    CONTAINER_NAME = "curated"

    # Initialize Data Lake client
    service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=os.environ["DATA_LAKE_KEY"],
    )
    file_system_client = service_client.get_file_system_client(
        file_system=CONTAINER_NAME
    )

    # Generate filepath with date partitioning
    date_str = kwargs["ds"]  # Use Airflow execution date
    file_path = f"energy_data/dt={date_str}/energy_data.csv"

    # Convert DataFrame to CSV
    csv_data = energy_df.to_csv(index=False).encode("utf-8")

    # Create the file
    file_client = file_system_client.create_file(file_path)
    file_client.append_data(data=csv_data, offset=0, length=len(csv_data))
    file_client.flush_data(len(csv_data))

    print(f"Successfully uploaded CSV to {CONTAINER_NAME}/{file_path}")
    return file_path


# Task 1: Download the CSV using BashOperator
download_task = BashOperator(
    task_id="download_energy_data",
    bash_command='wget -O /tmp/energy_data_{{ ds }}.csv $ENERGY_DATA_URL && echo "/tmp/energy_data_{{ ds }}.csv"',
    env={
        "ENERGY_DATA_URL": "{{ var.value.energy_data_url }}",
    },
    dag=dag,
)

# Task 2: Upload to Azure Data Lake using PythonOperator
upload_task = PythonOperator(
    task_id="upload_to_datalake",
    python_callable=upload_to_datalake,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
download_task >> upload_task
