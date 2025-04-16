from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from minio import Minio
import os
import pendulum
import pandas as pd
import sqlalchemy

def download_from_minio(**kwargs):
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "spark"
    os.makedirs("/tmp/data/raw", exist_ok=True)
    downloaded_files = []
    for obj in client.list_objects(bucket, recursive=True):
        if not obj.object_name.endswith('.parquet'):
            continue
        local_file = os.path.join("/tmp/data/raw", obj.object_name)
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        client.fget_object(bucket, obj.object_name, local_file)
        downloaded_files.append(local_file)
    kwargs['ti'].xcom_push(key='local_files', value=downloaded_files)

def insert_into_postgres(**kwargs):
    local_files = kwargs['ti'].xcom_pull(key='local_files')
    engine = sqlalchemy.create_engine('postgresql://postgres:admin@data-warehouse:5432/taxi')
    for local_file in local_files:
        df = pd.read_parquet(local_file)
        df.columns = df.columns.str.lower()
        df.to_sql('yellow_tripdata', engine, if_exists='append', index=False)
        os.remove(local_file)

with DAG(dag_id='Minio_to_Postgres',
         start_date=days_ago(1),
         schedule_interval=None,
         catchup=False,
         tags=['minio/read/write', 'postgres'],
         ) as dag2:
    t3 = PythonOperator(
        task_id='download_from_minio',
        provide_context=True,
        python_callable=download_from_minio
    )
    t4 = PythonOperator(
        task_id='insert_into_postgres',
        provide_context=True,
        python_callable=insert_into_postgres
    )
    t3 >> t4
