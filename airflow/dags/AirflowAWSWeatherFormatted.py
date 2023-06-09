# The dag is created to clean and preprocess weather data and place into the temporal formatted zone (formattedtemporal s3 bucket)
# Import required packages
from datetime import datetime, timedelta
import boto3
import os
import aws_hadoop
import pyspark
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Get the current date
today = datetime.today().date()

# Create a new datetime object with today's date and a start time of midnight
start_date = datetime.combine(today, datetime.min.time())


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": start_date,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}
dag = DAG("weather_data_formatted_zone", default_args=default_args, schedule_interval="0 3 * * *")

# set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("aws_access_key")
os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("aws_secret_access_key")

# create global variables for buckets
s3 = boto3.client("s3")
source_bucket = "growsmartpersistentlanding"
destination_bucket = "formattedtemporal"

# Get the current date and add 1 day
today = datetime.now()
today_str = today.strftime("%Y-%m-%d")
tomorrow = datetime.now() + timedelta(days=1)
tomorrow_str = tomorrow.strftime("%Y-%m-%d")

# Get the date in the YYYY-MM-DD format
date_str = tomorrow.strftime("%Y-%m-%d")


# Create a function to check the document presence in source S3 bucket
def check_weather_parquet_existence():
    # Check if the files exist in the source S3 bucket
    prefix = f"weather_data/{today_str}/"
    response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)
    files = [content["Key"] for content in response.get("Contents", []) if content["Key"].endswith(".parquet")]

    # If the files exist in the bucket, trigger the extract_and_transform task
    if files:
        print(f"The following parquet files exist in the source S3 bucket: {files}")
        return "clean_and_load_data"
    else:
        return "stop"


def clean_and_load_data():
    # Get all found files in the source S3 bucket
    prefix = f"weather_data/{today_str}/"
    response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)
    files = [content["Key"] for content in response.get("Contents", []) if content["Key"].endswith(".parquet")]
    print(files)

    # Create a SparkSession
    spark = SparkSession.builder.appName("parquet_rewrite").getOrCreate()

    # Load each file as a Spark dataframe, clean it, and store the cleaned data
    cleaned_data = None
    for file in files:
        # Read Parquet as a Spark dataframe from S3 and clean the data
        df = spark.read.parquet(f"s3a://{source_bucket}/{file}")
        cleaned_df = df.dropDuplicates()

        # Append the cleaned data to the merged dataframe
        if cleaned_data is None:
            cleaned_data = cleaned_df
        else:
            cleaned_data = cleaned_data.union(cleaned_df)

    # Repartition the cleaned data before saving as a CSV file
    cleaned_data = cleaned_data.repartition(1)  # Set the desired number of partitions

    # Save the merged data as a CSV file in S3
    csv_key = f"weather_data"
    cleaned_data.write.mode("overwrite").parquet(f"s3a://{destination_bucket}/{csv_key}")

    print(f"Cleaned data saved as {csv_key} in {destination_bucket}")

    return "stop"


# Create a function to check the document presence in destination S3 bucket
def check_weather_formatted_parquet_existence():
    prefix = f"weather_data/"
    response = s3.list_objects(Bucket=destination_bucket, Prefix=prefix)
    files = [content["Key"] for content in response.get("Contents", []) if content["Key"].lower().endswith(".parquet")]

    # If the files exist in the bucket, trigger the extract_and_transform task
    if files:
        print(f"The following parquet file exist in the destination S3 bucket: {files}")
    else:
        return "stop"


# Define the checking task
t1 = PythonOperator(
    task_id="check_weather_parquet_existence", python_callable=check_weather_parquet_existence, dag=dag
)


# Define the clean and load task
t2 = PythonOperator(
    task_id="clean_and_load_data",
    python_callable=clean_and_load_data,
    op_kwargs={"data": '{{ task_instance.xcom_pull(task_ids="check_weather_parqet_existence") }}'},
    dag=dag,
)

# Define the csv check task
t3 = PythonOperator(
    task_id="check_weather_formatted_parquet_existence",
    python_callable=check_weather_formatted_parquet_existence,
    op_kwargs={"data": '{{ task_instance.xcom_pull(task_ids="clean_and_load_data") }}'},
    dag=dag,
)

t1 >> t2 >> t3
