#import the required packages

from datetime import datetime
import boto3
import os
import io
import pandas as pd
import json
import logging

from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator


#set the credentials to connetcs to the buckets
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
dag = DAG('Iot_devices_data', default_args=default_args)

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'temporarydevicedata'
destination_bucket = 'growsmarttemporallanding'

#create a function to check the document presence in temporarydevicedata bucket
def check_documents_existence():

    # List all the files in the source S3 bucket
    response = s3.list_objects(Bucket=source_bucket)
    files = []
    for content in response.get('Contents', []):
        key = content.get('Key')
        if not key.endswith('/'):  # Skip directories
            files.append(f's3://temporarydevicedata/{key}')
    
    # If there are files in the bucket, trigger the extract_and_merge_data task
    if files:
        return 'extract_and_merge_data'
    else:
        return 'stop'

#create the function for extract and merge data
def extract_and_merge_data():

    # List all the files in the source S3 bucket
    response = s3.list_objects(Bucket=source_bucket)
    files = []
    for content in response.get('Contents', []):
        key = content.get('Key')
        if not key.endswith('/'):  # Skip directories
            files.append(f's3://temporarydevicedata/{key}')
    
    logging.info(response)

    if not files:
        logging.error('No files found in source bucket')
        return
    
    logging.info(f'Number of files found in source bucket: {len(files)}')
    
    # Read all files into a Pandas dataframe
    dfs = []
    for file in files:
        obj = s3.get_object(Bucket=source_bucket, Key=file.split('temporarydevicedata/')[1])
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


#create the function for deleting extracted files from the source bucket
def deletion_of_extracted_data():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(source_bucket)
    bucket.objects.all().delete()

#Define the checking task
check_documents_existence = PythonOperator(
    task_id='check_documents_existence',
    python_callable=check_documents_existence,
    dag=dag)

#Define the extract and merge task in the DAG
extract_and_merge_data = PythonOperator(
    task_id='extract_and_merge_data',
    python_callable=extract_and_merge_data,
    dag=dag)

deletion_of_extracted_data = PythonOperator(
    task_id='deletion_of_extracted_data',
    python_callable=deletion_of_extracted_data,
    dag=dag)

#create the flow
check_documents_existence >> extract_and_merge_data >> deletion_of_extracted_data