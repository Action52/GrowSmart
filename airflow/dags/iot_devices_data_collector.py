from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
import logging
import datetime
import boto3


def verify_s3_bucket(ti):
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    s3 = session.resource("s3")
    my_bucket = s3.Bucket("growsmartplanttraits")
    size = len(list(my_bucket.objects.all()))
    if size != 0:
        ti.xcom_push(
            key="plant_traits_csv_filename",
            value=[obj.key for obj in my_bucket.objects.all()],
        )
        logging.info("New file found. Executing rest of DAG.")
        return True
    logging.info("No new files found. Aborting.")
    return False


def move_document(ti):
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    s3 = session.resource("s3")
    my_bucket = s3.Bucket("growsmarttemporallanding")
    document_paths = ti.xcom_pull(key="plant_traits_csv_filename")
    filenames = []
    for i, document_path in enumerate(document_paths):
        copy_source = {"Bucket": "growsmartplanttraits", "Key": document_path}
        filename = f"{str(datetime.date.today())}/{i}_planttraits.csv"
        logging.info(
            f"Copying bucket {copy_source['Bucket']} file {copy_source['Key']} into"
            f" s3://growsmarttemporallanding/{filename}"
        )
        s3.meta.client.copy(copy_source, "growsmarttemporallanding", filename)
        filenames.append(filename)
    ti.xcom_push(key="plant_traits_new_files_landing", value=filenames)


def confirm_doc(ti):
    filenames = ti.xcom_pull(key="plant_traits_new_files_landing")
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    s3 = session.resource("s3")
    my_bucket = s3.Bucket("growsmarttemporallanding")
    objs = list(my_bucket.objects.all())
    keys = set(o.key for o in objs)
    for filename in filenames:
        if filename not in keys:
            raise FileNotFoundError("File not found in federated landing zone.")
    logging.info(f"All {filenames} in federated landing zone. Correct.")
    return True


default_args = {
    "owner": "Luis Alfredo Leon",
    "depends_on_past": False,
    "start_date": datetime.datetime.now(),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "plant_traits_data_collector",
    max_active_runs=1,
    default_args=default_args,
    description=(
        "This pipeline extracts the csv from the plant traints organization and places it on the "
        "temporal landing zone."
    ),
)

begin_task = EmptyOperator(task_id="Begin", dag=dag)

verify_new_document_task = ShortCircuitOperator(
    task_id="verify_new_doc_in_pipeline",
    dag=dag,
    python_callable=verify_s3_bucket,
    do_xcom_push=True,
)

move_new_document_task = PythonOperator(task_id="move_doc_to_landing_zone", dag=dag, python_callable=move_document)

confirm_docs_in_landing_task = ShortCircuitOperator(
    task_id="confirm_plant_traits_doc_in_pipeline", dag=dag, python_callable=confirm_doc
)

end_task = EmptyOperator(task_id="End", dag=dag)

(begin_task >> verify_new_document_task >> move_new_document_task >> confirm_docs_in_landing_task >> end_task)