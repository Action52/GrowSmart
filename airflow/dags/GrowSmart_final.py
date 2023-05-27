#Import required packages
from datetime import datetime, timedelta
import boto3
import os
import pandas as pd
import pyarrow.parquet as pq
import aws_hadoop
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import re
from pyspark.sql.functions import col
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
dag = DAG('GrowSmart_final', default_args=default_args, schedule_interval='0 4 * * *')

#set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'formattedtemporal'
destination_bucket = 'growsmartformattedzone'
 # Create a SparkSession
spark = SparkSession.builder.appName("join").getOrCreate()

# Find the folder with the latest date in the source S3 bucket for iot
def join_iot_weather():
    # Get one file from iot_data folder
    iot_response = s3.list_objects(Bucket=source_bucket, Prefix='iot_data/', Delimiter='/')
    iot_files = [content['Key'] for content in iot_response.get('Contents', []) if content['Key'].endswith('.csv')]
    iot_file = iot_files[0] if iot_files else None

    # Get one file from weather_data folder
    weather_response = s3.list_objects(Bucket=source_bucket, Prefix='weather_data/', Delimiter='/')
    weather_files = [content['Key'] for content in weather_response.get('Contents', []) if content['Key'].endswith('.csv')]
    weather_file = weather_files[0] if weather_files else None

    if not iot_file or not weather_file:
        print("No IoT or weather data file found.")
        return 'stop'

    # Read the files as Spark DataFrames
    iot_df = spark.read.csv(f's3a://{source_bucket}/{iot_file}', header=True)
    weather_df = spark.read.csv(f's3a://{source_bucket}/{weather_file}', header=True)

    # Perform the full join based on the "location" and "city" columns
    joined_df = iot_df.join(weather_df, iot_df["location"] == weather_df["city"], "full")
    
    # Repartition the DataFrame into a single partition
    joined_df = joined_df.repartition(1)
    
    # Save the merged data as a CSV file in S3
    joined_df.write.mode("overwrite").csv(f's3a://{destination_bucket}/joined_data/', header=True)

    print(f"Cleaned data saved in {destination_bucket}/joined_data/")

    return 'stop'

#Define tasks
join_iot_weather_task= PythonOperator(
    task_id = 'join_iot_weather',
    python_callable = join_iot_weather,
    dag = dag)


# Define a dummy task to use as a joiner
start = DummyOperator(task_id="latest_folders", dag=dag)

# Define a dummy task to use as a joiner
finish = DummyOperator(task_id="parquets", dag=dag)

#Create process flow

start >> join_iot_weather_task >> finish