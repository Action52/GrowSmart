#The dag is created to extract the weather prediction (for the next 3 days) using
#Free Weather API https://open-meteo.com/ with the schedule every 3 days for Barcelona


#Import the packeges

from datetime import datetime, , timedelta
import json
import boto3
import requests
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

#set credentials to acces AWS s3 bucket
#sustitute YOUR_AWS_ACCESS_KEY_ID and YOUR_AWS_SECRET_ACCESS_KEY with your own values

os.environ['AWS_ACCESS_KEY_ID'] = 'YOUR_AWS_ACCESS_KEY_ID'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'YOUR_AWS_SECRET_ACCESS_KEY'

#set the defauld args for the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval': '@daily'
}

#create the dag

dag = DAG('website_data_extraction_s3', default_args=default_args, schedule_interval=timedelta(days=3))

#create a function to access the weather data using API

def extract_data():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.39&longitude=2.16&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,precipitation,rain,soil_temperature_54cm,soil_moisture_9_27cm&forecast_days=3'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise ValueError(f'Request failed.')

#create a function to upload the weather data inro AWS s3 bucket

def upload_to_s3(data):
    s3 = boto3.client('s3')
    bucket_name = 'growsmarttemporallanding'
    s3_key = 'website_data.json'
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