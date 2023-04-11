import os
import io
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import boto3

### Please install s3fs

#set the credentials to connetcs to the buckets
os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")


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

# Define a function to verify the CSV document exists in S3
def verify_csv_exists(bucket_name, key_name):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=key_name)
    except:
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
    # Read CSV file from S3
    s3 = boto3.client('s3')
    csv_obj = s3.get_object(Bucket=bucket_name, Key=input_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(io.StringIO(csv_string))
    
    # Convert dataframe to Parquet format
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    # Write Parquet file to S3
    s3.put_object(Body=parquet_buffer, Bucket=bucket_name, Key=output_key)
    return True

# Define a PythonOperator task to convert the CSV file to Parquet format
convert_to_parquet_task = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_to_parquet,
    op_kwargs={'bucket_name': 'growsmarttemporallanding', 'input_key': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv', 'output_key': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.parquet'},
    dag=dag,
)

def move_and_delete_files():
    # Move parquet file
    source_bucket = 'growsmarttemporallanding'
    dest_bucket = 'growsmartpersistentlanding'
    parquet_key = f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.parquet'
    
    s3 = boto3.client('s3')
    s3.copy_object(Bucket=dest_bucket, CopySource=f'{source_bucket}/{parquet_key}', Key=parquet_key)
    s3.delete_object(Bucket=source_bucket, Key=parquet_key)

    # Delete csv file
    csv_key = f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv'
    s3.delete_object(Bucket=source_bucket, Key=csv_key)
    
move_and_delete_task = PythonOperator(
    task_id='move_and_delete_files',
    python_callable=move_and_delete_files,
    dag=dag,
)

# Set task dependencies
verify_csv_task >> convert_to_parquet_task >> move_and_delete_task
