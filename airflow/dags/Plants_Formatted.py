# The dag is created to clean and preprocess plant data and place into the temporal formatted zone (formattedtemporal s3 bucket) 

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
from pyspark.sql.functions import when, col


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
destination_bucket = 'formattedtemporal'



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

    # Add a new column 'plant_name' based on 'Species_name_standardized_against_TPL'
    cleaned_df = cleaned_df.withColumn("plant_name", 
                                       when(col("Species_name_standardized_against_TPL") == "Spinacia oleracea", "Spinach")
                                       .when(col("Species_name_standardized_against_TPL") == "Allium fistulosum", "Green onion/Scallions")
                                       .when(col("Species_name_standardized_against_TPL") == "Allium sativum", "Garlic")
                                       .when(col("Species_name_standardized_against_TPL") == "Allium cepa", "Onion")
                                       .when(col("Species_name_standardized_against_TPL") == "Daucus carota", "Carrot")
                                       .when(col("Species_name_standardized_against_TPL") == "Petroselinum crispum", "Parsley")
                                       .when(col("Species_name_standardized_against_TPL") == "Eruca vesicaria", "Arugula")
                                       .when(col("Species_name_standardized_against_TPL") == "Brassica oleracea", "Kale")
                                       .when(col("Species_name_standardized_against_TPL") == "Lactuca sativa", "Lettuce")
                                       .when(col("Species_name_standardized_against_TPL") == "Cucumis sativus", "Cucumber")
                                       .when(col("Species_name_standardized_against_TPL") == "Basilicum polystachyon", "Basil")
                                       .when(col("Species_name_standardized_against_TPL") == "Mentha piperita", "Mint")
                                       .when(col("Species_name_standardized_against_TPL") == "Origanum vulgare", "Oregano")
                                       .when(col("Species_name_standardized_against_TPL") == "Pisum sativum", "Peas")
                                       .when(col("Species_name_standardized_against_TPL") == "Phaseolus vulgaris", "Green bean")
                                       .when(col("Species_name_standardized_against_TPL") == "Fragaria ananassa", "Strawberry")
                                       .when(col("Species_name_standardized_against_TPL") == "Solanum lycopersicum", "Tomatoes")
                                       .when(col("Species_name_standardized_against_TPL") == "Solanum melongena", "Eggplant")
                                       .when(col("Species_name_standardized_against_TPL") == "Capsicum annuum", "Bell pepper")
                                       .when(col("Species_name_standardized_against_TPL") == "Capsicum chinense", "Chilli pepper")
                                       .when(col("Species_name_standardized_against_TPL") == "Solanum tuberosum", "Potatoes")
                                       .otherwise("Unknown"))
    
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



