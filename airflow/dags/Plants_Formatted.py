#Import required packages
from datetime import datetime, timedelta
from io import BytesIO
import boto3
import os
import pandas as pd
import pyarrow.parquet as pq
import aws_hadoop
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import re
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}
dag = DAG('plants_data_formatted_zone', default_args=default_args, schedule_interval=None)

#set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'growsmartpersistentlanding'
destination_bucket = 'growsmartformattedzone'



# Find the folder with the latest date in the source S3 bucket
def latest_folder():

    #List all folders inside of the plant_traits folder of the sourse bucket
    response = s3.list_objects(Bucket=source_bucket, Prefix='plant_traits/', Delimiter='/')

    folder_names = []
    for o in response.get('CommonPrefixes'):
        folder_name = o.get('Prefix')
        print('sub folder:', folder_name)
        folder_names.append(folder_name)

    latest_date = max(folder_names, key=lambda x: datetime.strptime(x.split('/')[1], "%Y-%m-%d"))
    latest_date = latest_date.split('/')[1]
    print('Latest date:', latest_date)

    return latest_date
    

# Check for parquet files in subfolders of the latest_date folder
def parquet_files():
    latest_date = latest_folder()
    
    # Check for parquet files in subfolders of the latest_date folder
    prefix = f'plant_traits/{latest_date}/'
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

# Extract the plant data from a specific folders
def data_extraction():
    latest_date = latest_folder()

    families = ['Apiaceae', 'Amaryllidaceae', 'Amaranthaceae', 'Brassicaceae', 'Compositae', 'Lamiaceae', 'Leguminosae', 'Rosaceae', 'Solanaceae']
    files = []

    # Iterate through the families
    for family in families:
        prefix = f'plant_traits/{latest_date}/Family={family}/'
        response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)

        family_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.parquet')]
        files.extend(family_files)

        print(f"The Parquet files in {prefix} are: {family_files}")

    # Create a SparkSession
    spark = SparkSession.builder.appName("parquet_merge").getOrCreate()

    dfs = []
    for file in files:
        # Read Parquet as a Spark DataFrame from S3
        df = spark.read.parquet(f's3a://{source_bucket}/{file}')

        # Print the column names before renaming
        print("Column names before renaming:")
        print(df.columns)

        # Rename the columns using acceptable format
        new_column_names = [re.sub(r'[ ,;{}()\n\t=`"]', '_', col) for col in df.columns]
        print(new_column_names)

        # Rename the columns in the DataFrame
        for idx, col in enumerate(df.columns):
            df = df.withColumnRenamed(col, new_column_names[idx])
        print(df.columns)

        dfs.append(df)

    # Merge all DataFrames into a single DataFrame
    merged_df = reduce(DataFrame.unionByName, dfs)

    # Apply dropDuplicates() to the merged DataFrame
    cleaned_df = merged_df.dropDuplicates()

    # Repartition, save, and return the result
    cleaned_data = cleaned_df.repartition(1)
    cleaned_data.write.mode("overwrite").csv(f's3a://{destination_bucket}/plant_traits', header=True)

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



