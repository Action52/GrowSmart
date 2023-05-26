
# The dag is created to extract the weather prediction (for next day)
# Free Weather API https://open-meteo.com/ with the schedule every day for 5 cities in Spain (Barcelona, Girona, Tarragona, Lleida and Madrid)

#Import the packeges

from datetime import datetime, timedelta
import json
import csv
import io
import boto3
import requests
import os

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator

#set credentials to acces AWS s3 bucket. Credentials are taken from airflow variables

os.environ['AWS_ACCESS_KEY_ID'] = Variable.get("aws_access_key")
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get("aws_secret_access_key")

# Get the current date
today = datetime.today().date()

# Create a new datetime object with today's date and a start time of midnight
start_date = datetime.combine(today, datetime.min.time())

#set the defauld args for the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

#create the dag

dag = DAG('AirflowAWSWeather', default_args=default_args, schedule_interval = '0 1 * * *')


#create functions to access the weather data using API

def extract_data_Barcelona():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.39&longitude=2.16&daily=temperature_2m_max,temperature_2m_min,rain_sum,showers_sum,snowfall_sum,precipitation_probability_max&forecast_days=1&timezone=Europe%2FBerlin'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Barcelona'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Girona():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.98&longitude=2.82&daily=temperature_2m_max,temperature_2m_min,rain_sum,showers_sum,snowfall_sum,precipitation_probability_max&forecast_days=1&timezone=Europe%2FBerlin'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Girona'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Madrid():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=40.42&longitude=-3.70&daily=temperature_2m_max,temperature_2m_min,rain_sum,showers_sum,snowfall_sum,precipitation_probability_max&forecast_days=1&timezone=Europe%2FBerlin'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Madrid'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Tarragona():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.12&longitude=1.25&daily=temperature_2m_max,temperature_2m_min,rain_sum,showers_sum,snowfall_sum,precipitation_probability_max&forecast_days=1&timezone=Europe%2FBerlin'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Tarragona'
        return data
    else:
        raise ValueError(f'Request failed.')

def extract_data_Lleida():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=41.62&longitude=0.62&daily=temperature_2m_max,temperature_2m_min,rain_sum,showers_sum,snowfall_sum,precipitation_probability_max&forecast_days=1&timezone=Europe%2FBerlin'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data['city'] = 'Lleida'
        return data
    else:
        raise ValueError(f'Request failed.')

#Create function to combine the data
def combine_data():
    # Call all the extract_data functions and store the results in a list
    city_data = [extract_data_Barcelona(), extract_data_Girona(), extract_data_Madrid(), extract_data_Tarragona(), extract_data_Lleida()]

    # Combine all the city data into one dictionary
    all_data = {}
    for data in city_data:
        city_name = data['city']
        # Add a new column 'prediction_date' to the data dictionary with a value of current date + 1 day
        current_date = datetime.now().date()
        pr_date = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')
        prediction_date = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')
        data['pr_date'] = pr_date
        data['prediction_date'] = prediction_date
        all_data[city_name] = data

    return all_data

#Create function to upload the data in s3
def upload_to_s3():
    all_data = combine_data()
    s3 = boto3.client('s3')
    bucket_name = 'growsmarttemporallanding'
    s3_key = 'weather_data.csv'

    # Delete all existing files with name website_data.csv in the s3 bucket
    files_to_delete = s3.list_objects(Bucket=bucket_name, Prefix=s3_key)
    delete_keys = {'Objects': []}
    if 'Contents' in files_to_delete:
        for obj in files_to_delete['Contents']:
            delete_keys['Objects'].append({'Key': obj['Key']})
        s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    # Delete the existing file with the same name if it exists
    s3.delete_object(Bucket=bucket_name, Key=s3_key)

    # Convert the dictionary to CSV data
    csv_data = io.StringIO()
    writer = csv.writer(csv_data)
    writer.writerow(
        ['pr_date', 'prediction_date', 'city', 'temperature_2m_max', 'temperature_2m_min', 'rain_sum', 'showers_sum',
         'snowfall_sum', 'precipitation_probability_max'])
    for city_data in all_data.values():
        city_name = city_data['city']
        row = [city_data['pr_date'], city_data['prediction_date']]
        row += [city_name, city_data['daily']['temperature_2m_max'][0], city_data['daily']['temperature_2m_min'][0],
                city_data['daily']['rain_sum'][0], city_data['daily']['showers_sum'][0],
                city_data['daily']['snowfall_sum'][0], city_data['daily']['precipitation_probability_max'][0]]
        writer.writerow(row)

    # Write the CSV data to the S3 bucket
    s3.put_object(Body=csv_data.getvalue().encode('utf-8'), Bucket=bucket_name, Key=s3_key)

#Create function to check the correct loading of the weather_data.csv to the s3 temporal bucket
def check_file():
    s3 = boto3.client('s3')
    bucket_name = 'growsmarttemporallanding'
    s3_key = 'weather_data.csv'

    # Scan all existing files with name website_data.csv in the s3 bucket
    file_to_scan = s3.list_objects(Bucket=bucket_name, Prefix=s3_key)
    if 'Contents' in file_to_scan:
        return True
    else:
        return False

#Create tasks 'extract_data_task' to extract the data

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

#Create task 'combine_data' to upload the data
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

#Create task 'check_file' to upload the data
t4 = PythonOperator(
    task_id ='check_file',
    python_callable = check_file,
    op_kwargs = {'data': '{{ task_instance.xcom_pull(task_ids="upload_to_s3_task") }}'},
    dag=dag,
)

#Create the the task flow


[t1_a, t1_b, t1_c, t1_d, t1_e] >> t2 >> t3 >> t4
