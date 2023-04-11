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

dag = DAG('AirflowAWSWeather', default_args=default_args, schedule_interval=timedelta(days=3))

#create functions to access the weather data using API

def extract_data_Barcelona():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.39&longitude=2.16&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,precipitation,rain,soil_temperature_54cm,soil_moisture_9_27cm&forecast_days=3'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Barcelona'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Girona():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.98&longitude=2.82&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,precipitation,rain,soil_temperature_54cm,soil_moisture_9_27cm&forecast_days=3'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Girona'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Madrid():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=40.42&longitude=-3.70&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,precipitation,rain,soil_temperature_54cm,soil_moisture_9_27cm&forecast_days=3'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Madrid'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Tarragona():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.12&longitude=1.25&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,precipitation,rain,soil_temperature_54cm,soil_moisture_9_27cm&forecast_days=3'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Tarragona'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Lleida():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.62&longitude=0.62&hourly=temperature_2m,relativehumidity_2m,precipitation_probability,precipitation,rain,soil_temperature_54cm,soil_moisture_9_27cm&forecast_days=3'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Lleida'
        return data
    else:
        raise ValueError(f'Request failed.')

def combine_data():
    # Call all the extract_data functions and store the results in a list
    city_data = [extract_data_Barcelona(), extract_data_Girona(), extract_data_Madrid(), extract_data_Tarragona(), extract_data_Lleida()]

    # Combine all the city data into one dictionary
    all_data = {}
    for data in city_data:
        city_name = data['city']
        all_data[city_name] = data
    return all_data

#create a function to upload the weather data inro AWS s3 bucket

def upload_to_s3():
    all_data = combine_data()
    s3 = boto3.client('s3')
    bucket_name = 'growsmarttemporallanding'
    s3_key = 'weather_data.json'
     
    # Delete all existing files with name website_data.json in the s3 bucket
    files_to_delete = s3.list_objects(Bucket=bucket_name, Prefix=s3_key)
    delete_keys = {'Objects': []}
    if 'Contents' in files_to_delete:
        for obj in files_to_delete['Contents']:
            delete_keys['Objects'].append({'Key': obj['Key']})
        s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    # Upload the new file
    s3.put_object(Body=json.dumps(all_data), Bucket=bucket_name, Key=s3_key)

#Create task 'extract_data_task' to extract the data

t1_a = PythonOperator(
    task_id='extract_data_Barcelona',
    python_callable=extract_data_Barcelona,
    dag=dag,
)

t1_b = PythonOperator(
    task_id='extract_data_Girona',
    python_callable=extract_data_Girona,
    dag=dag,
)

t1_c = PythonOperator(
    task_id='extract_data_Madrid',
    python_callable=extract_data_Madrid,
    dag=dag,
)

t1_d = PythonOperator(
    task_id='extract_data_Tarragona',
    python_callable=extract_data_Tarragona,
    dag=dag,
)

t1_e = PythonOperator(
    task_id='extract_data_Lleida',
    python_callable=extract_data_Lleida,
    dag=dag,
)

#Create task 'upload_to_s3_task' to upload the data
t2 = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data,
    dag=dag,
)

#Create task 'upload_to_s3_task' to upload the data
t3 = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_to_s3,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="combine_data") }}'},
    dag=dag,
)


#Create the the task flow


[t1_a, t1_b, t1_c, t1_d, t1_e] >> t2 >> t3

