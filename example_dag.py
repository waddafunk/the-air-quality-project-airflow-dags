"""
Example Airflow DAG with common patterns
------------------------------------------------
This DAG demonstrates:
- Multiple task types (BashOperator, PythonOperator)
- Task dependencies with various methods
- Scheduling configuration
- Documentation
- Templating
- Default arguments
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_workflow',
    default_args=default_args,
    description='An example DAG with typical patterns',
    schedule_interval='0 0 * * *',  # Run once a day at midnight
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example', 'tutorial'],
    doc_md="""
    ## Example Workflow DAG
    
    This DAG demonstrates common Airflow patterns for reference.
    
    ### Tasks included:
    * start_workflow: Empty operator to mark the beginning
    * fetch_data: Bash operator to simulate data fetching
    * process_data: Python function to process the simulated data
    * Task group with parallel validation tasks
    * notify: Task to simulate notification
    * end_workflow: Empty operator to mark the end
    """
)

def _process_data(**context):
    """
    Example Python function for data processing.
    In a real scenario, this would contain your data processing logic.
    """
    print("Processing data...")
    # Example of pulling task data from previous tasks
    fetch_output = context['ti'].xcom_pull(task_ids='fetch_data')
    print(f"Received data from fetch_data: {fetch_output}")
    
    # Example of pushing data for downstream tasks
    processed_data = "Processed data result"
    context['ti'].xcom_push(key='processed_data', value=processed_data)
    
    return "Data processing completed successfully"

def _validate_data_completeness(**context):
    """Example validation function"""
    print("Validating data completeness...")
    return "Validation passed"

def _validate_data_quality(**context):
    """Example validation function"""
    print("Validating data quality...")
    return "Validation passed"

def _send_notification(**context):
    """Example notification function"""
    print("Sending notification...")
    # Could be replaced with email, Slack, or other notification logic
    return "Notification sent"

# Define tasks
start_workflow = EmptyOperator(
    task_id='start_workflow',
    dag=dag,
)

fetch_data = BashOperator(
    task_id='fetch_data',
    bash_command='echo "Fetching data from source..." && sleep 5 && echo "Data fetch complete with {{ ts }}"',
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=_process_data,
    provide_context=True,
    dag=dag,
)

# Creating a task group for related tasks
with TaskGroup(group_id='validation_tasks', dag=dag) as validation_group:
    validate_completeness = PythonOperator(
        task_id='validate_completeness',
        python_callable=_validate_data_completeness,
        provide_context=True,
    )
    
    validate_quality = PythonOperator(
        task_id='validate_quality',
        python_callable=_validate_data_quality,
        provide_context=True,
    )

notify = PythonOperator(
    task_id='notify',
    python_callable=_send_notification,
    provide_context=True,
    dag=dag,
)

end_workflow = EmptyOperator(
    task_id='end_workflow',
    dag=dag,
)

# Define the task dependencies
start_workflow >> fetch_data >> process_data >> validation_group >> notify >> end_workflow