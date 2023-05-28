#Import required packages
from datetime import datetime, timedelta
import boto3
import os
from pyspark.sql.functions import rand
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pyspark.sql import functions as F
from airflow.models import Variable
from pyspark.sql.functions import lit, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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

# Function to set nullable flag to True for all columns in a schema
def set_nullable(schema):
    for field in schema.fields:
        field.nullable = True
    return schema

def join():
    # Get one file from iot_data folder
    iot_response = s3.list_objects(Bucket=source_bucket, Prefix='iot_data/', Delimiter='/')
    iot_files = [content['Key'] for content in iot_response.get('Contents', []) if content['Key'].endswith('.csv')]
    iot_file = iot_files[0] if iot_files else None

    # Get one file from weather_data folder
    weather_response = s3.list_objects(Bucket=source_bucket, Prefix='weather_data/', Delimiter='/')
    weather_files = [content['Key'] for content in weather_response.get('Contents', []) if content['Key'].endswith('.csv')]
    weather_file = weather_files[0] if weather_files else None

    # Get one file from plants_data folder
    plants_response = s3.list_objects(Bucket=source_bucket, Prefix='plant_traits/', Delimiter='/')
    plants_files = [content['Key'] for content in plants_response.get('Contents', []) if content['Key'].endswith('.csv')]
    plants_file = plants_files[0] if plants_files else None

    if not iot_file or not weather_file or not plants_file:
        print("No IoT, weather, or plants data file found.")
        return 'stop'

    # Read the files as Spark DataFrames
    iot_df = spark.read.csv(f's3a://{source_bucket}/{iot_file}', header=True)
    weather_df = spark.read.csv(f's3a://{source_bucket}/{weather_file}', header=True)
    plants_df = spark.read.csv(f's3a://{source_bucket}/{plants_file}', header=True)

    # Perform the join between iot_df and weather_df
    joined_df = iot_df.join(weather_df, iot_df["location"] == weather_df["city"], "inner")

    # Cross join with plants_df to add random rows from plants_df to each row in joined_df
    cross_joined_df = joined_df.crossJoin(plants_df)

    # Add row number within each group of unique sensor_id and garden_name combinations
    window = Window.partitionBy(col("sensor_id"), col("garden_name")).orderBy(rand())
    cross_joined_df = cross_joined_df.withColumn("row_number", row_number().over(window))

    # Filter the rows based on the specified criteria
    cross_joined_df = cross_joined_df.filter(
        (col("row_number").between(1, 5) & (col("garden_name") == "Big Size Garden")) |
        (col("row_number").between(1, 2) & (col("garden_name") == "Small Garden With Lamp")) |
        (col("row_number").between(1, 3) & (col("garden_name") == "Middle Size Garden"))
    )

    cross_joined_df = cross_joined_df.repartition(1)

    # Save the merged data as a CSV file in S3
    cross_joined_df.write.mode("overwrite").csv(f's3a://{destination_bucket}/{today}/', header=True)

    print(f"Cleaned data saved in {destination_bucket}/joined_data/")

    return 'stop'

# Create a function to check the document presence in destination S3 bucket
def check_formatted_csv_existence():

    prefix = f'{today}/'
    response = s3.list_objects(Bucket=destination_bucket, Prefix=prefix)
    files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]

    # If the files exist in the bucket, trigger the extract_and_transform task
    if files:
        print(f"The following csv file exists in the destination S3 bucket: {files}")
    else:
        return ("The csv file was not found in the destination S3 bucket")

# Create a function to check the document presence in destination S3 bucket
def delete_temporal_files():
    folders = ['iot_data/', 'plant_traits/', 'weather_data/']

    for folder in folders:
        prefix = folder
        response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', [])]

        if files:
            # Delete the files in the folder
            delete_objects = [{'Key': file} for file in files]
            s3.delete_objects(Bucket=source_bucket, Delete={'Objects': delete_objects})
            print(f"The following files have been deleted in the {folder} folder: {files}")
        else:
            print(f"No files found in the {folder} folder.")

    return 'stop'

#Define tasks
join_iot_weather_task= PythonOperator(
    task_id = 'join',
    python_callable = join,
    dag = dag)

check_formatted_csv_existence_task= PythonOperator(
    task_id = 'check_formatted_csv_existence',
    python_callable = check_formatted_csv_existence,
    dag = dag)

delete_temporal_files_task= PythonOperator(
    task_id = 'delete_temporal_files',
    python_callable = delete_temporal_files,
    dag = dag)

# Define a dummy task to use as a joiner
start = DummyOperator(task_id="start", dag=dag)

# Define a dummy task to use as a joiner
finish = DummyOperator(task_id="finish", dag=dag)

#Create process flow

start >> join_iot_weather_task >> check_formatted_csv_existence_task >> delete_temporal_files_task >> finish 