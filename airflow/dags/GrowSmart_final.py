# The dag is created to join iot, plant and wather data from temporal formatted zone and place into the formatted zone (growsmartformattedzone s3 bucket)
# Import required packages
from datetime import datetime, timedelta
import boto3
import os
from pyspark.sql.functions import rand
from airflow import DAG
from airflow.operators.python import PythonOperator
try:
    from airflow.operators.dummy_operator import DummyOperator
except:
    from airflow.operators.empty import EmptyOperator
from pyspark.sql import functions as F
from airflow.models import Variable
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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
dag = DAG("GrowSmart_final", default_args=default_args, schedule_interval="0 4 * * *")

# set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("aws_access_key")
os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("aws_secret_access_key")

# create global variables for buckets
s3 = boto3.client("s3")
source_bucket = "formattedtemporal"
destination_bucket = "growsmartformattedzone"
# Create a SparkSession
spark = SparkSession.builder.appName("join").getOrCreate()


# Function to set nullable flag to True for all columns in a schema
def set_nullable(schema):
    for field in schema.fields:
        field.nullable = True
    return schema


def join():
    # Get one file from iot_data folder
    iot_response = s3.list_objects(Bucket=source_bucket, Prefix="iot_data/", Delimiter="/")
    iot_files = [
        content["Key"] for content in iot_response.get("Contents", []) if content["Key"].lower().endswith(".parquet")
    ]
    iot_file = iot_files[0] if iot_files else None

    # Get one file from weather_data folder
    weather_response = s3.list_objects(Bucket=source_bucket, Prefix="weather_data/", Delimiter="/")
    weather_files = [
        content["Key"]
        for content in weather_response.get("Contents", [])
        if content["Key"].lower().endswith(".parquet")
    ]
    weather_file = weather_files[0] if weather_files else None

    # Get one file from plants_data folder
    plants_response = s3.list_objects(Bucket=source_bucket, Prefix="plant_traits/", Delimiter="/")
    plants_files = [
        content["Key"]
        for content in plants_response.get("Contents", [])
        if content["Key"].lower().endswith(".parquet")
    ]
    plants_file = plants_files[0] if plants_files else None

    if not iot_file or not weather_file or not plants_file:
        print("No IoT, weather, or plants data file found.")
        return "stop"

    # Read the files as Spark DataFrames
    iot_df = spark.read.parquet(f"s3a://{source_bucket}/{iot_file}")
    weather_df = spark.read.parquet(f"s3a://{source_bucket}/{weather_file}")
    plants_df = spark.read.parquet(f"s3a://{source_bucket}/{plants_file}")

    # Specify column names for the duplicate columns
    iot_df_with_alias = iot_df.select([col(c).alias(f"iot_{c}") for c in iot_df.columns])
    weather_df_with_alias = weather_df.select([col(c).alias(f"weather_{c}") for c in weather_df.columns])
    plants_df_with_alias = plants_df.select([col(c).alias(f"plants_{c}") for c in plants_df.columns])

    # Perform the join between iot_df and weather_df
    joined_df = iot_df_with_alias.join(
        weather_df_with_alias, iot_df_with_alias["iot_location"] == weather_df_with_alias["weather_city"], "inner"
    )

    # Convert columns to integer type
    joined_df = joined_df.withColumn("iot_species_id", joined_df["iot_species_id"].cast("integer"))
    print(joined_df.show(1))
    print(joined_df.printSchema())
    plants_df_with_alias = plants_df_with_alias.withColumn(
        "plants_Species_id", plants_df_with_alias["plants_Species_id"].cast("integer")
    )
    print(plants_df_with_alias.show(1))
    print(plants_df_with_alias.printSchema())

    print(joined_df.select("iot_species_id").show(1))
    print(plants_df_with_alias.select("plants_Species_id").show(1))

    # Perform the join between joined_df and plants_df
    final_df = joined_df.join(
        plants_df_with_alias, joined_df["iot_species_id"] == plants_df_with_alias["plants_Species_id"], "inner"
    )
    final_df = final_df.repartition(1)
    print(final_df.show(1))

    # Save the merged data as a CSV file in S3
    final_df.write.mode("overwrite").parquet(f"s3a://{destination_bucket}/{today}/")

    print(f"Cleaned data saved in {destination_bucket}/joined_data/")

    return "stop"


# Create a function to check the document presence in destination S3 bucket
def check_formatted_parquet_existence():
    prefix = f"{today}/"
    response = s3.list_objects(Bucket=destination_bucket, Prefix=prefix)
    files = [content["Key"] for content in response.get("Contents", []) if content["Key"].lower().endswith(".parquet")]

    # If the files exist in the bucket, trigger the extract_and_transform task
    if files:
        print(f"The following parquet file exists in the destination S3 bucket: {files}")
    else:
        return "The csv file was not found in the destination S3 bucket"


# Create a function to check the document presence in destination S3 bucket
def delete_temporal_files():
    folders = ["iot_data/", "plant_traits/", "weather_data/"]

    for folder in folders:
        prefix = folder
        response = s3.list_objects(Bucket=source_bucket, Prefix=prefix)
        files = [content["Key"] for content in response.get("Contents", [])]

        if files:
            # Delete the files in the folder
            delete_objects = [{"Key": file} for file in files]
            s3.delete_objects(Bucket=source_bucket, Delete={"Objects": delete_objects})
            print(f"The following files have been deleted in the {folder} folder: {files}")
        else:
            print(f"No files found in the {folder} folder.")

    return "stop"


# Define tasks
join_iot_weather_task = PythonOperator(task_id="join", python_callable=join, dag=dag)

check_formatted_csv_existence_task = PythonOperator(
    task_id="check_formatted_csv_existence", python_callable=check_formatted_parquet_existence, dag=dag
)

delete_temporal_files_task = PythonOperator(
    task_id="delete_temporal_files", python_callable=delete_temporal_files, dag=dag
)

# Define a dummy task to use as a joiner
start = DummyOperator(task_id="start", dag=dag)

# Define a dummy task to use as a joiner
finish = DummyOperator(task_id="finish", dag=dag)

# Create process flow

start >> join_iot_weather_task >> check_formatted_csv_existence_task >> delete_temporal_files_task >> finish
