from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 25),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_submit_operator',
    default_args=default_args,
    description='An Airflow DAG to submit a PySpark job',
    schedule_interval=timedelta(days=1),
)

aws_access_key_id = Variable.get("aws_access_key")
aws_secret_access_key = Variable.get("aws_secret_access_key")

t1 = SparkSubmitOperator(
    task_id='spark_submit_job',
    application='spark-scripts/process_s3_formatted.py',
    conn_id='spark-container-connector',
    application_args=['s3a://growsmartformattedzone/', 'pocFormatted.csv', aws_access_key_id, aws_secret_access_key],
    dag=dag
)

t1
