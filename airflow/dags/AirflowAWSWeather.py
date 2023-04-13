#The dag is created to extract the weather prediction (for the next 3 days) using
#Free Weather API https://open-meteo.com/ with the schedule every 3 days for Barcelona


#Import the packeges

from datetime import datetime, timedelta
import json
import boto3
import requests
import os

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
    'start_date': datetime(2023, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

#create the dag

dag = DAG('website_data_extraction_s3', default_args=default_args, schedule_interval=timedelta(days=3))

#create a function to access the weather data using API

def extract_data():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.39&longitude=2.16&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,precipitation,rain,soil_temperature_54cm,soil_moisture_9_27cm&forecast_days=3'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Barcelona'
        return data
    else:
        raise ValueError(f'Request failed.')

#create a function to upload the weather data inro AWS s3 bucket

def upload_to_s3(data):
    s3 = boto3.client('s3')
    bucket_name = 'growsmarttemporallanding'
    s3_key = 'website_data.json'
     
    # Delete all existing files with name website_data.json in the s3 bucket
    files_to_delete = s3.list_objects(Bucket=bucket_name, Prefix=s3_key)
    delete_keys = {'Objects': []}
    if 'Contents' in files_to_delete:
        for obj in files_to_delete['Contents']:
            delete_keys['Objects'].append({'Key': obj['Key']})
        s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    # Upload the new file
    s3.put_object(Body=json.dumps(data), Bucket=bucket_name, Key=s3_key)

#Create task 'extract_data_task' to extract the data

t1 = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag,
)

#Create task 'upload_to_s3_task' to upload the data
t2 = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_to_s3,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="extract_data_task") }}'},
    dag=dag,
)


#Create the the task flow

t1 >> t2