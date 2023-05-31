# The dag is created to clean and preprocess iot data and place into the temporal formatted zone (formattedtemporal s3 bucket) 
#Import required packages
from datetime import datetime, timedelta
from io import BytesIO
import boto3
import re
import os
import pandas as pd
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


# Get the current date
today = datetime.today().date()

# Create a new datetime object with today's date and a start time of midnight
start_date = datetime.combine(today, datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}
dag = DAG('iot_formatted_zone', default_args=default_args, schedule_interval='0 4 * * *')

#set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'growsmartpersistentlanding'
destination_bucket = 'formattedtemporal'



# Find the folder with the latest date in the source S3 bucket
def latest_folder():
    # List all folders inside the iot_data folder of the source bucket
    response = s3.list_objects(Bucket=source_bucket, Prefix='iot_data/', Delimiter='/')

    folder_names = []
    for o in response.get('CommonPrefixes'):
        folder_name = o.get('Prefix')
        print('Sub folder:', folder_name)
        folder_names.append(folder_name)

    latest_folder = max(folder_names, key=lambda x: datetime.strptime(x.split('iot_data_')[1].split('.parquet/')[0], "%Y-%m-%d"))
    latest_date = latest_folder.split('iot_data_')[1].split('.parquet/')[0]
    print('Latest folder:', latest_folder)
    print('Latest date:', latest_date)

    return latest_folder
   

# Check for parquet files in subfolders of the latest_date folder
def parquet_files():
    latest_folder_name = latest_folder()

    # Check for parquet files in subfolders of the latest_date folder
    prefix = f'{latest_folder_name}'
    response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)

    parquet_files = []
    for content in response.get('Contents', []):
        key = content['Key']
        if key.endswith('.parquet'):
            parquet_files.append(key)

    if parquet_files:
        print(f"The following parquet files exist in the source S3 bucket: {parquet_files}")
    else:
        print('No parquet files found.')

    return parquet_files

def data_extraction():
    latest_folder_name = latest_folder()

    # Check for parquet files in subfolders of the latest_folder_name folder
    prefix = f'{latest_folder_name}'
    response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)

    parquet_files = []
    for content in response.get('Contents', []):
        key = content['Key']
        if key.endswith('.parquet'):
            parquet_files.append(key)

    if not parquet_files:
        print('No parquet files found.')
        return None

    # Create a SparkSession
    spark = SparkSession.builder.appName("parquet_merge").getOrCreate()

    dfs = []
    for file in parquet_files:
        # Read Parquet as a Spark DataFrame from S3
        df = spark.read.parquet(f's3a://{source_bucket}/{file}')

        # Perform data cleaning or other transformations as needed

        # Extract the sensor_id from the subfolder name
        sensor_id = file.split('/')[-2].split('=')[1]

        # Add the sensor_id column
        df = df.withColumn('sensor_id', lit(sensor_id))

        dfs.append(df)

    # Merge all DataFrames into a single DataFrame
    merged_df = reduce(DataFrame.unionByName, dfs)

    # Perform additional data cleaning or transformations as needed

    # Remove duplicates based on all columns
    cleaned_df = merged_df.dropDuplicates()

    # Repartition, save, and return the cleaned DataFrame as CSV
    cleaned_data = cleaned_df.coalesce(1)
    cleaned_data.write.mode("overwrite").csv(f's3a://{destination_bucket}/iot_data', header=True)

    return 'stop'



#Define the checking task
t1 = PythonOperator(
    task_id='latest_folder',
    python_callable=latest_folder,
    dag=dag)

#Define the parquet file check task
t2 = PythonOperator(
    task_id='parquet_files',
    python_callable=parquet_files,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="latest_folder") }}'},
    dag=dag)

#Define the extraction data task
t3 = PythonOperator(
    task_id='data_extraction',
    python_callable=data_extraction,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="parquet_files") }}'},
    dag=dag)

t1 >> t2 >> t3



