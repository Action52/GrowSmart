import os
import pandas as pd
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 11),
    'depends_on_past': False,
    'catchup': False,
}

# Define the DAG
dag = DAG(
    'iot_device_persistent_data',
    default_args=default_args,
    description='Convert CSV to Parquet and upload to S3',
    schedule_interval='0 0 * * *',  # Run every day at midnight
    max_active_runs=1,
)

# Initialize the S3Hook
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
s3_hook = S3Hook(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Define a function to verify the CSV document exists in S3
def verify_csv_exists(bucket_name, key_name):
    csv_exists = s3_hook.check_for_key(key=key_name, bucket_name=bucket_name)
    if not csv_exists:
        raise ValueError(f"CSV file {key_name} does not exist in S3 bucket {bucket_name}")
    else:
        return True

# Define a PythonOperator task to verify the CSV document exists in S3
verify_csv_task = PythonOperator(
    task_id='verify_csv_exists',
    python_callable=verify_csv_exists,
    op_kwargs={'bucket_name': 'growsmarttemporallanding', 'key_name': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv'},
    dag=dag,
)

# Define a function to convert the CSV file to Parquet format
def convert_to_parquet(bucket_name, input_key, output_key):
    df = pd.read_csv(f's3://{bucket_name}/{input_key}')
    s3_hook.load_dataframe(df, output_key, bucket_name=bucket_name, partition_cols=None, use_threads=True)
    return True

# Define a PythonOperator task to convert the CSV file to Parquet format
convert_to_parquet_task = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_to_parquet,
    op_kwargs={'bucket_name': 'growsmarttemporallanding', 'input_key': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv', 'output_key': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.parquet'},
    dag=dag,
)

# Define a BashOperator task to move the Parquet file to another S3 bucket
move_parquet_task = BashOperator(
    task_id='move_parquet',
    bash_command=f'aws s3 mv s3://growsmarttemporallanding/iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.parquet s3://growsmartpersistentlanding/iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.parquet && aws s3 rm s3://growsmarttemporallanding/iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv',
    dag=dag,
)

# Set task dependencies
verify_csv_task >> convert_to_parquet_task >> move_parquet_task
