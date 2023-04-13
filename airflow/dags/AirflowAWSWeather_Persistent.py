from datetime import datetime, timedelta
import csv
import boto3
import requests
import os
from io import StringIO, BytesIO
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator

#set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")

#set the defauld args for the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 11, 1, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

#create the dag

dag = DAG('AirflowAWSWeather_Persistent', default_args=default_args, schedule_interval='0 2 * * *')

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'growsmarttemporallanding'
destination_bucket = 'growsmartpersistentlanding'

# Create a function to check the document presence in source S3 bucket
def check_documents_existence():
    # Check if the file exists in the source S3 bucket
    response = s3.list_objects(Bucket=source_bucket, Prefix='weather_data')
    files = [content['Key'] for content in response.get('Contents', [])]
    
    # If the file exists in the bucket, trigger the extract_and_transform task
    if files:
        return 'extract_and_transform'
    else:
        return 'stop'

# Create the function for extract and transform data
def extract_and_transform():
    # Get input_data.csv from the source S3 bucket
    response = s3.get_object(Bucket=source_bucket, Key='weather_data.csv')
    content = response['Body'].read().decode('utf-8')

    # Parse the CSV content into a Pandas DataFrame
    df = pd.read_csv(StringIO(content))

    # Convert non-numeric data types to strings
    for column in df.columns:
        if df[column].dtype != 'float64' and df[column].dtype != 'int64':
            df[column] = df[column].astype(str)

    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write the table to a Parquet file in memory
    buf = BytesIO()
    pq.write_table(table, buf)


    # Upload the Parquet file to the destination S3 bucket
    buf.seek(0)
    key = 'weather_data.parquet'
    s3.upload_fileobj(Fileobj=buf, Bucket=destination_bucket, Key=key)

    return 'transform_complete'


#Define the checking task
t1 = PythonOperator(
    task_id='check_documents_existence',
    python_callable=check_documents_existence,
    dag=dag)


#Define the extract and merge task in the DAG
t2 = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform,
    dag=dag)

t1 >> t2



