from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

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


# Define the function to upload to Azure Data Lake
def upload_to_datalake(ti, **kwargs):
    # Get the downloaded file path from XCom
    downloaded_file_path = ti.xcom_pull(task_ids=["download_energy_data"])[0]

    # Read the CSV file
    energy_df = pd.read_csv(downloaded_file_path)
    print(f"Read {len(energy_df)} rows from energy data file")

    # Storage account parameters - use a fixed name based on infrastructure naming pattern
    STORAGE_ACCOUNT_NAME = os.environ["PREFIX"] + "dls" + os.environ["ENVIRONMENT"]
    STORAGE_ACCOUNT_NAME = STORAGE_ACCOUNT_NAME.replace("-", "")
    CONTAINER_NAME = "curated"

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


# Task 1: Download the CSV using PythonOperator
download_task = PythonOperator(
    task_id="download_energy_data",
    python_callable=download_energy_data,
    provide_context=True,
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

if __name__ == "__main__":
    dag.test()
