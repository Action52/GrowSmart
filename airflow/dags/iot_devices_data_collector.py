from datetime import datetime
import boto3
import os
import io
import pandas as pd
import json
import logging
import ast

from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.exceptions import AirflowException

#set the credentials to connect to the buckets
os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")

#set default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval': '@daily'
}

#create a dag
dag = DAG('iot_devices_data', default_args=default_args)

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'temporarydevicedata'
destination_bucket = 'growsmarttemporallanding'

def extract_and_merge_data(ti):

    # Get the list of files from the XCom
    files = ti.xcom_pull(key='files')

    logging.info(f'Number of files found in source bucket: {len(files)}')

    if not files:
        logging.error('No files found in source bucket')
        return
    
    # Read all files into a Pandas dataframe
    dfs = []
    for file in files:
        obj = s3.get_object(Bucket=source_bucket, Key=file)
        content = obj['Body'].read()
        data = json.loads(content)
        df = pd.DataFrame(data, index=[0])
        dfs.append(df)
    
    logging.info(f'Number of dataframes merged: {len(dfs)}')
    
    # Concatenate all dataframes into one
    merged_df = pd.concat(dfs)

    # Write the merged dataframe to a CSV file in memory
    output = io.StringIO()
    merged_df.to_csv(output, index=False)
    csv_bytes = output.getvalue().encode('utf-8')

    # Get the current date
    date = datetime.now().strftime('%Y-%m-%d')

    # Upload the merged CSV file to the destination S3 bucket
    s3.put_object(Bucket=destination_bucket, Key=f'iot_data_{date}/iot_data.csv', Body=csv_bytes)
    
    logging.info(f'Size of merged dataframe: {merged_df.shape}')
    logging.info(f'Merged CSV file uploaded to {destination_bucket}')

    # Pass the key to the next task
    ti.xcom_push(key='csv_key', value=f'iot_data_{date}/iot_data.csv')
    
def deletion_of_extracted_data(keys):
    s3 = boto3.resource('s3')
    keys = ast.literal_eval(keys)
    objects_to_delete = [{'Key': key} for key in keys]
    response = s3.meta.client.delete_objects(Bucket=source_bucket, Delete={'Objects': objects_to_delete})
    deleted_objects = [obj['Key'] for obj in response.get('Deleted', [])]

    if len(deleted_objects) != len(keys):
        logging.error(f"Failed to delete some objects: {keys} {deleted_objects}")
        raise AirflowException("Failed to delete objects")

    logging.info(f"Deleted {len(deleted_objects)} objects from source bucket {source_bucket}")
    

def list_files_in_bucket(bucket_name):
    response = s3.list_objects(Bucket=bucket_name)
    files = []
    for content in response.get('Contents', []):
        key = content.get('Key')
        if not key.endswith('/'):  # Skip directories
            files.append(key)
    return files

def check_data_in_landing_zone():
    # Get the current date
    date = datetime.now().strftime('%Y-%m-%d')
    
    # List all the files in the destination S3 bucket
    files = list_files_in_bucket(destination_bucket)
    
    # If there are files in the bucket, check if the CSV file exists
    if files:
        if f'iot_data_{date}/iot_data.csv' in files:
            return 'success'
        else:
            return 'failure'
    else:
        return 'failure'

def check_documents_existence(ti):
    files = list_files_in_bucket(source_bucket)
    
    # If there are files in the bucket, trigger the extract_and_merge_data task
    if files:
        ti.xcom_push(key='files', value=files)
        return 'extract_and_merge_data'
    else:
        return 'stop'

# Create the ShortCircuitOperator
check_documents_existence_op = ShortCircuitOperator(
    task_id='check_documents_existence',
    python_callable=check_documents_existence,
    dag=dag
)

# Define the rest of the tasks in the DAG
extract_and_merge_data_op = PythonOperator(
    task_id='extract_and_merge_data',
    python_callable=extract_and_merge_data,
    provide_context=True,  # Pass the TaskInstance to the callable
    dag=dag
)

deletion_of_extracted_data_op = PythonOperator(
    task_id='deletion_of_extracted_data',
    python_callable=deletion_of_extracted_data,
    op_kwargs={'keys': "{{ ti.xcom_pull(key='files') }}"},
    dag=dag
)

check_data_in_landing_zone_op = PythonOperator(
    task_id='check_data_in_landing_zone',
    python_callable=check_data_in_landing_zone,
    provide_context=True,  # Pass the TaskInstance to the callable
    dag=dag
)

check_documents_existence_op >> extract_and_merge_data_op >> deletion_of_extracted_data_op >> check_data_in_landing_zone_op
