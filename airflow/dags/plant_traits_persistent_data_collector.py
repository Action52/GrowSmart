from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
import logging
import datetime
import boto3
import pandas as pd
import csv


def verify_s3_bucket(ti):
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    s3 = session.resource("s3")
    my_bucket = s3.Bucket("growsmarttemporallanding")
    objects = list(my_bucket.objects.filter(Prefix="plant_traits/"))
    size = len(objects)
    if size != 0:
        ti.xcom_push(
            key="temporal_plant_traits_csv_filename",
            value=[obj.key for obj in objects],
        )
        logging.info("New file found. Executing rest of DAG.")
        return True
    logging.info("No new files found. Aborting.")
    return False


def review_new_docs_schema(ti):
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    filenames = ti.xcom_pull(key="temporal_plant_traits_csv_filename")
    expected_columns = [
        "Species_id",
        "Species_name_standardized_against_TPL",
        "Taxonomic_level",
        "Status_according_to_TPL",
        "Genus",
        "Family",
        "Phylogenetic_Group_within_angiosperms",
        "Phylogenetic_Group_General",
        "Adaptation_to_terrestrial_or_aquatic_habitats",
        "Woodiness",
        "Growth_Form",
        "Succulence",
        "Nutrition_type_parasitism",
        "Nutrition_type_carnivory",
        "Leaf_type",
        "Leaf_area_mm2",
        "Leaf_area_n_o",
        "Nmass_mg_g",
        "Nmass_n_o",
        "LMA_g_m2",
        "LMA_n_o",
        "Plant_height_m",
        "Plant_height_n_o",
        "Diaspore_mass_mg",
        "Diaspore_mass_n_o",
        "SSD_observed_mg_mm3",
        "SSD_n_o",
        "LDMC_g_g",
        "LDMC_n_o",
        "SSD_imputed_mg_mm3",
        "SSD_combined_mg_mm3",
        "Number_of_traits_with_values",
    ]

    ti.xcom_push(key="expected_cols", value=expected_columns)
    s3 = session.client("s3")
    for i, filename in enumerate(filenames):
        logging.debug(f"s3://growsmarttemporallanding/{filename}")
        df = pd.read_csv(
            f"s3://growsmarttemporallanding/{filename}",
            storage_options={"key": Variable.get("aws_access_key"), "secret": Variable.get("aws_secret_access_key")},
        )
        logging.debug(df.head())
        for col in expected_columns:
            logging.info(f"Checking if col {col} in {filename} headers.")
            assert col in df.columns
    logging.info("Correct")


def transform_and_move_datafile(ti):
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    s3 = session.client("s3")
    filenames = ti.xcom_pull(key="temporal_plant_traits_csv_filename")
    moved_files = []
    for i, filename in enumerate(filenames):
        df = pd.read_csv(
            f"s3://growsmarttemporallanding/{filename}",
            storage_options={"key": Variable.get("aws_access_key"), "secret": Variable.get("aws_secret_access_key")},
        )
        expected_cols = ti.xcom_pull(key="expected_cols")
        df = df[expected_cols]
        fn = filename.strip(".csv")
        today = str(datetime.date.today())

        df.to_parquet(
            f"s3://growsmartpersistentlanding/plant_traits/{today}",
            partition_cols=["Family"],
            storage_options={"key": Variable.get("aws_access_key"), "secret": Variable.get("aws_secret_access_key")},
        )
        moved_files.append(f"plant_traits/{today}")
    ti.xcom_push(key="plant_traits_new_files_persistent", value=moved_files)


def confirm_doc(ti):
    filenames = ti.xcom_pull(key="plant_traits_new_files_persistent")
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    s3 = session.resource("s3")
    my_bucket = s3.Bucket("growsmartpersistentlanding")
    for filename in filenames:
        logging.info(f"Checking if {filename} in persistent landing zone.")
        objects = list(my_bucket.objects.filter(Prefix=f"{filename}/"))
        logging.info(objects)
        if len(objects) == 0:
            raise FileNotFoundError("File not found in federated persistent zone.")
    logging.info(f"All {filenames} in federated landing zone. Correct.")
    return True


def delete_docs_from_landing(ti):
    session = boto3.Session(
        region_name="eu-west-3",
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )
    s3 = session.resource("s3")
    filenames = ti.xcom_pull(key="temporal_plant_traits_csv_filename")
    for filename in filenames:
        obj = s3.Object("growsmarttemporallanding", filename)
        obj.delete()
        print(f"{filename} has been deleted from growsmarttemporallanding.")


default_args = {
    "depends_on_past": False,
    "start_date": datetime.datetime.now(),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "plant_traits_persistent_data_collector",
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

review_new_docs_schema_task = PythonOperator(
    task_id="review_new_docs_schema", dag=dag, python_callable=review_new_docs_schema
)

transform_and_move_task = PythonOperator(
    task_id="transform_and_move_csvs", dag=dag, python_callable=transform_and_move_datafile
)

confirm_docs_in_persistent_task = ShortCircuitOperator(
    task_id="confirm_plant_traits_doc_in_persistent_zone", dag=dag, python_callable=confirm_doc
)

delete_docs_from_landing_task = PythonOperator(
    task_id="delete_docs_from_landing", dag=dag, python_callable=delete_docs_from_landing
)

end_task = EmptyOperator(task_id="End", dag=dag)

(
    begin_task
    >> verify_new_document_task
    >> review_new_docs_schema_task
    >> transform_and_move_task
    >> confirm_docs_in_persistent_task
    >> delete_docs_from_landing_task
    >> end_task
)
