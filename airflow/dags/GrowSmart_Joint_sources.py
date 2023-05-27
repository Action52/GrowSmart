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
dag = DAG('GrowSmart_Formatted', default_args=default_args, schedule_interval='0 4 * * *')

#set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")

#create global variables for buckets
s3 = boto3.client('s3')
source_bucket = 'growsmartpersistentlanding'
destination_bucket = 'growsmartformattedzone'

# Get the current date and add 1 day
today = datetime.now()
today_str = today.strftime('%Y-%m-%d')
tomorrow = datetime.now() + timedelta(days=1)
tomorrow_str = tomorrow.strftime('%Y-%m-%d')

# Get the date in the YYYY-MM-DD format
date_str = tomorrow.strftime('%Y-%m-%d')

# Create a spark session for a dag
spark = SparkSession.builder.appName("GrowSmart_Formatted").getOrCreate()

# Find the folder with the latest date in the source S3 bucket for iot
def iot_latest_folder():
    # List all folders inside the iot_data folder of the source bucket
    iot_response = s3.list_objects(Bucket=source_bucket, Prefix='iot_data/', Delimiter='/')

    iot_folder_names = []
    for o in iot_response.get('CommonPrefixes'):
        iot_folder_name = o.get('Prefix')
        print('Sub folder:', iot_folder_name)
        iot_folder_names.append(iot_folder_name)

    iot_latest_folder = max(iot_folder_names, key=lambda x: datetime.strptime(x.split('iot_data_')[1].split('.parquet/')[0], "%Y-%m-%d"))
    iot_latest_date = iot_latest_folder.split('iot_data_')[1].split('.parquet/')[0]
    print('IOT Latest folder:', iot_latest_folder)
    print('IOT Latest date:', iot_latest_date)

    return iot_latest_folder

# Find the folder with the latest date in the source S3 bucket for plants
def plants_latest_folder():

    #List all folders inside of the plant_traits folder of the sourse bucket
    plants_response = s3.list_objects(Bucket=source_bucket, Prefix='plant_traits/', Delimiter='/')

    plants_folder_names = []
    for o in plants_response.get('CommonPrefixes'):
        plants_folder_name = o.get('Prefix')
        print('sub folder:', plants_folder_name)
        plants_folder_names.append(plants_folder_name)

    plants_latest_folder= max(plants_folder_names, key=lambda x: datetime.strptime(x.split('/')[1], "%Y-%m-%d"))
    plants_latest_folder = plants_latest_folder.split('/')[1]
    print('Plant Latest folder:', plants_latest_folder)

    return plants_latest_folder

# For weather data the search for the latest date folder is not required because the current date is used. 
# Check weather_parquet_files() function 

# Check for parquet files for iot
def iot_parquet_files():
    iot_latest_folder_name = iot_latest_folder()

    # Check for parquet files in subfolders of the latest_date folder
    iot_prefix = f'{iot_latest_folder_name}'
    iot_response_i = s3.list_objects(Bucket=source_bucket, Prefix=iot_prefix)

    iot_parquet_files = []
    for content in iot_response_i.get('Contents', []):
        key = content['Key']
        if key.endswith('.parquet'):
            iot_parquet_files.append(key)

    if iot_parquet_files:
        print(f"The following parquet files exist in the source S3 bucket: {iot_parquet_files}")
    else:
        print('No parquet files found.')

    return iot_parquet_files

# Check for parquet files for weather
def weather_parquet_files():
    # Check if the files exist in the source S3 bucket
    weather_prefix = f'weather_data/{today_str}/'
    weather_response = s3.list_objects(Bucket=source_bucket, Prefix=weather_prefix)
    weather_parquet_files = [content['Key'] for content in weather_response.get('Contents', []) if content['Key'].endswith('.parquet')]

    # If the files exist in the bucket, trigger the extract_and_transform task
    if weather_parquet_files:
        print(f"The following parquet files exist in the source S3 bucket: {weather_parquet_files}")
    else:
        print('No parquet files found.')
    
    return 'weather_parquet_files'

# Check for parquet files for plants
def plants_parquet_files():
    plants_latest_folder_name = plants_latest_folder()
    
    # Check for parquet files in subfolders of the latest_date folder
    plants_prefix_i = f'plant_traits/{plants_latest_folder_name}/'
    plants_response_i = s3.list_objects(Bucket=source_bucket, Prefix=plants_prefix_i)

    plants_parquet_files = []
    for content in plants_response_i.get('Contents', []):
        key = content['Key']
        if key.endswith('.parquet'):
            plants_parquet_files.append(key)

    if plants_parquet_files:
        print(f"The following parquet files exist in the source S3 bucket: {plants_parquet_files}")
    else:
        print('No parquet files found.')

    return plants_parquet_files

# IOT data extraction and cleaning
def iot_data_extraction():
    latest_folder_name = iot_latest_folder()

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

        # Extract the sensor_id from the subfolder name
        sensor_id = file.split('/')[-2].split('=')[1]

        # Add the sensor_id column
        df = df.withColumn('sensor_id', lit(sensor_id))

        dfs.append(df)

    # Merge all DataFrames into a single DataFrame
    merged_df = reduce(DataFrame.unionByName, dfs)

    # Remove duplicates based on all columns
    cleaned_df = merged_df.dropDuplicates()

    # Repartition, save, and return the cleaned DataFrame as CSV
    cleaned_data = cleaned_df.coalesce(1)

    iot_cleaned_data = cleaned_df.toPandas()

    return iot_cleaned_data


# Weather data extraction and cleaning
def weather_data_extraction():
    # Get all found files in the source S3 bucket
    prefix = f'weather_data/{today_str}/'
    response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)
    files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.parquet')]
    print(files)

    # Load each file as a Spark dataframe, clean it, and store the cleaned data
    cleaned_data = None
    for file in files:
        # Read Parquet as a Spark dataframe from S3 and clean the data
        df = spark.read.parquet(f's3a://{source_bucket}/{file}')
        cleaned_df = df.dropDuplicates()
        
        # Append the cleaned data to the merged dataframe
        if cleaned_data is None:
            cleaned_data = cleaned_df
        else:
            cleaned_data = cleaned_data.union(cleaned_df)

    if cleaned_data is not None:
        # Repartition the cleaned data before saving as a CSV file
        cleaned_data = cleaned_data.repartition(1)  # Set the desired number of partitions

    return cleaned_data


# Plants data extraction and cleaning
def plants_data_extraction():
    latest_date = plants_latest_folder()
    folders = ['Apiaceae', 'Amaryllidaceae', 'Amaranthaceae', 'Brassicaceae', 'Compositae', 'Cucurbitaceae', 'Lamiaceae', 'Leguminosae', 'Rosaceae', 'Solanaceae']

    files = []
    for folder in folders:
        prefix = f'plant_traits/{latest_date}/Family={folder}/'
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

        # Filter data based on specific values in Species_name_standardized_against_TPL column
        species_names = ['Spinacia oleracea', 'Allium fistulosum', 'Allium sativum', 'Allium cepa',
                         'Daucus carota', 'Petroselinum crispum', 'Eruca vesicaria', 'Brassica oleracea',
                         'Lactuca sativa', 'Cucumis sativus', 'Basilicum polystachyon', 'Mentha piperita',
                         'Origanum vulgare', 'Pisum sativum', 'Phaseolus vulgaris', 'Fragaria ananassa',
                         'Solanum lycopersicum', 'Solanum melongena', 'Capsicum annuum', 'Capsicum chinense',
                         'Solanum tuberosum']

        df = df.filter(df["Species_name_standardized_against_TPL"].isin(species_names))

        dfs.append(df)

    # Merge all DataFrames into a single DataFrame
    merged_df = reduce(DataFrame.unionByName, dfs)

    # Select only desired species from the merged DataFrame
    selected_species = ['Spinacia oleracea', 'Allium fistulosum', 'Allium sativum', 'Allium cepa',
                        'Daucus carota', 'Petroselinum crispum', 'Eruca vesicaria', 'Brassica oleracea',
                        'Lactuca sativa', 'Cucumis sativus', 'Basilicum polystachyon', 'Mentha piperita',
                        'Origanum vulgare', 'Pisum sativum', 'Phaseolus vulgaris', 'Fragaria ananassa',
                        'Solanum lycopersicum', 'Solanum melongena', 'Capsicum annuum', 'Capsicum chinense',
                        'Solanum tuberosum']

    cleaned_df = merged_df.filter(merged_df["Species_name_standardized_against_TPL"].isin(selected_species))

    # Repartition, save, and return the result
    plants_cleaned_data = cleaned_df.repartition(1)

    plants_cleaned_data  = plants_cleaned_data .toPandas()

    return plants_cleaned_data
        
#Define tasks
iot_latest_folder_task = PythonOperator(
    task_id = 'iot_latest_folder',
    python_callable = iot_latest_folder,
    dag = dag)

plants_latest_folder_task = PythonOperator(
    task_id = 'plants_latest_folder',
    python_callable = plants_latest_folder,
    dag = dag)

iot_parquet_files_task = PythonOperator(
    task_id = 'iot_parquet_files',
    python_callable = iot_parquet_files,
    dag = dag)

weather_parquet_files_task = PythonOperator(
    task_id = 'weather_parquet_files',
    python_callable = weather_parquet_files,
    dag = dag)

plants_parquet_files_task = PythonOperator(
    task_id = 'plants_parquet_files',
    python_callable = plants_parquet_files,
    dag = dag)

iot_data_extraction_task = PythonOperator(
    task_id = 'iot_data_extraction',
    python_callable = iot_data_extraction,
    dag = dag)

weather_data_extraction_task = PythonOperator(
    task_id = 'weather_data_extraction',
    python_callable = weather_data_extraction,
    dag = dag)

plants_data_extraction_task = PythonOperator(
    task_id = 'plants_data_extraction',
    python_callable = plants_data_extraction,
    dag = dag)

# Define a dummy task to use as a joiner
latest_folders = DummyOperator(task_id="latest_folders", dag=dag)

# Define a dummy task to use as a joiner
parquets = DummyOperator(task_id="parquets", dag=dag)

extraction = DummyOperator(task_id="extraction", dag=dag)

# Define a dummy task to use as a joiner
join_task = DummyOperator(task_id="join_task", dag=dag)



#Create process flow

latest_folders >> [iot_latest_folder_task, plants_latest_folder_task] >> parquets >> [iot_parquet_files_task, weather_parquet_files_task, plants_parquet_files_task] >> extraction >> [iot_data_extraction_task, weather_data_extraction_task, plants_data_extraction_task] >> join_task

