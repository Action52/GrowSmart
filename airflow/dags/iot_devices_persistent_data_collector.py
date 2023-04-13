import os
import io
import pandas as pd
import logging
import boto3

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime


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
    schedule_interval='10 0 * * *',  # Run every day at 12:10 AM
    max_active_runs=1,
)

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'growsmarttemporallanding'
destination_bucket = 'growsmartpersistentlanding'

# Define a function to verify the CSV document exists in S3
def verify_csv_exists(bucket_name, key_name):
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
    op_kwargs={'bucket_name': source_bucket, 'key_name': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv'},
    dag=dag,
)

# Define a function to convert the CSV file to Parquet format
def convert_and_move_to_parquet(ti, input_key):
    # Read CSV file from S3
    df = pd.read_csv(f"s3://growsmarttemporallanding/{input_key}")
    
    # Convert dataframe to Parquet format and partition by id column
    output_key = f's3://growsmartpersistentlanding/iot_data/iot_data_{datetime.today().strftime("%Y-%m-%d")}.parquet'
    df.to_parquet(output_key, partition_cols=['device_id'])
    
    # Push the output key to xcom using ti
    parquet_key = f'iot_data/iot_data_{datetime.today().strftime("%Y-%m-%d")}.parquet'
    ti.xcom_push(key='uploaded_parquet', value=parquet_key)
    
    return True


# Define a PythonOperator task to convert the CSV file to Parquet format
convert_and_move_to_parquet_task = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_and_move_to_parquet,
    provide_context=True,
    op_kwargs={'bucket_name': source_bucket, 'input_key': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv', 'output_key': f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.parquet'},
    dag=dag,
)

def delete_files():
    csv_key = f'iot_data_{datetime.today().strftime("%Y-%m-%d")}/iot_data.csv'
    s3.delete_object(Bucket=source_bucket, Key=csv_key)
    
    return True
    
delete_task = PythonOperator(
    task_id='delete_files',
    python_callable=delete_files,
    dag=dag,
)

def verify_parquet(ti):
    key_name = ti.xcom_pull(key='uploaded_parquet')
    try:
        response = s3.list_objects_v2(Bucket=destination_bucket, Prefix=f"{key_name}/")
        logging.info(f"Getting response {response}")
        if 'Contents' not in response or len(response['Contents']) == 0:
            raise FileNotFoundError("File not found in federated persistent zone.")
    except:
        raise ValueError(f"Parquet file {key_name} does not exist in S3 bucket {destination_bucket}")
    else:
        return True
    
verify_parquet_task = PythonOperator(
    task_id='verify_parquet_exists',
    python_callable=verify_parquet,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
verify_csv_task >> convert_and_move_to_parquet_task >> verify_parquet_task >> delete_task
