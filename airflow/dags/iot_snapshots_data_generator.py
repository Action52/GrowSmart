from airflow import DAG
try:
    from airflow.operators.bash_operator import BashOperator
except:
    from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the relative path to the script
relative_path = "/scripts/device.py"

# Get the current date
today = datetime.today().date()
# Create a new datetime object with today's date and a start time of midnight
start_date = datetime.combine(today, datetime.min.time())

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "depends_on_past": False,
    "catchup": False,
}

# Define the DAG
dag = DAG(
    "iot_snapshots_data",
    default_args=default_args,
    description="Generate data",
    schedule_interval="0 9,18 * * *",  # Run every day at 9 and 18
    max_active_runs=1,
)

bash_operator = BashOperator(
    task_id="call_python_script", bash_command=f"python {relative_path} --s3 --airflow --max_stream 100", dag=dag
)

bash_operator
