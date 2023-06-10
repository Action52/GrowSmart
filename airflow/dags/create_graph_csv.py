from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import boto3
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 31),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logging.getLogger().setLevel(logging.DEBUG)

aws_access_key_id = Variable.get("aws_access_key")
aws_secret_access_key = Variable.get("aws_secret_access_key")
s3_bucket = "growsmartformattedzone"
s3_prefix = "2023-06-01"  # datetime.now().strftime('%Y-%m-%d')

dag = DAG("spark_submit_dag", default_args=default_args, schedule_interval=timedelta(1))


def list_s3_files(**kwargs):
    s3 = boto3.client(
        "s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name="eu-west-3"
    )
    files = []
    resp = s3.list_objects(Bucket=s3_bucket, Prefix=s3_prefix)
    if "Contents" in resp:
        for obj in resp["Contents"]:
            files.append(obj["Key"])
        if len(files) == 0:
            raise Exception("No files found in S3 bucket for the given prefix")
        logging.info("FILE TO USE: ", s3_prefix)
        kwargs["ti"].xcom_push(key="file_path", value=s3_prefix)
        # Save the prefix in xcom if it exists in s3
        kwargs["ti"].xcom_push(key="s3_prefix", value=s3_prefix)
    else:
        logging.info("No objects found for the given prefix")


list_files_task = PythonOperator(task_id="list_s3_files", python_callable=list_s3_files, provide_context=True, dag=dag)

spark_task = SparkSubmitOperator(
    task_id="spark_submit_task",
    conn_id="spark-container-connector",
    application="/scripts/process_s3_formatted.py",
    application_args=[
        "spark://127.0.0.1:7077",
        f"s3a://{s3_bucket}",
        "{{ti.xcom_pull(task_ids='list_s3_files', key='file_path')}}",
        f"{aws_access_key_id}",
        f"{aws_secret_access_key}",
    ],
    conf={
        "spark.jars": "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar",
        "spark.jars.packages": "net.java.dev.jets3t:jets3t:0.9.3",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": aws_access_key_id,
        "spark.hadoop.fs.s3a.secret.key": aws_secret_access_key,
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4",
        "fs.s3a.endpoint": "s3.eu-west-3.amazonaws.com",
        "spark.driver.host": "airflow-airflow-worker-1",
        "spark.driver.memory": "2G",
        "spark.executor.memory": "1200M",
        "spark.executor.memoryOverhead": "800M",
        "spark.sql.streaming.checkpointLocation": "/opt/bitnami/spark/"
        # "spark.network.timeout":"500s",
        # "spark.executor.heartbeatInterval":"480s"
    },
    dag=dag,
    verbose=True,
)

command_nodes = f"""
    source GrowSmart/spark/scripts/growsmart/bin/activate
    python3.9 bulkloader.py \
        {aws_access_key_id} \
        {aws_secret_access_key} \
        nodes/{datetime.now().strftime('%Y-%m-%d')} \
        arn:aws:iam::531971980327:role/s3GrowSmartRole
"""

command_edges = f"""
    source GrowSmart/spark/scripts/growsmart/bin/activate
    python3.9 bulkloader.py \
        {aws_access_key_id} \
        {aws_secret_access_key} \
        edges/{datetime.now().strftime('%Y-%m-%d')} \
        arn:aws:iam::531971980327:role/s3GrowSmartRole
"""

stream_nodes_to_neptune_task = SSHOperator(
    dag=dag, task_id="stream_nodes_to_neptune", ssh_conn_id="neptune_proxy", command=command_nodes
)

stream_edges_to_neptune_task = SSHOperator(
    dag=dag, task_id="stream_edges_to_neptune", ssh_conn_id="neptune_proxy", command=command_edges
)

list_files_task >> spark_task >> stream_nodes_to_neptune_task >> stream_edges_to_neptune_task
