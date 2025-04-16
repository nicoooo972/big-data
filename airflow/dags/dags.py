# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error


def download_parquet(**kwargs):
    folder_path: str = '/tmp/data/raw'
    os.makedirs(folder_path, exist_ok=True)
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"
    start = pendulum.datetime(2025, 1, 1)
    for i in range(3):
        month = start.subtract(months=i).format('YYYY-MM')
        file_url = url + filename + '_' + month + extension
        local_file = os.path.join(folder_path, f"yellow_tripdata_{month}.parquet")
        try:
            urllib.request.urlretrieve(file_url, local_file)
            print(f"Le fichier a été téléchargé avec succès dans {local_file}")
        except urllib.error.URLError as e:
            print(f"Failed to download {file_url}: {str(e)}")


# Python Function
def upload_file(**kwargs):
    ###############################################
    # Upload generated file to Minio

    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    # bucket: str = 'rawnyc'
    bucket: str = 'spark'
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"
    start = pendulum.datetime(2025, 1, 1)
    for i in range(3):
        month = start.subtract(months=i).format('YYYY-MM')
        object_name = filename + month + extension
        local_file = os.path.join("/tmp/data/raw", f"yellow_tripdata_{month}.parquet")
        if os.path.exists(local_file):
            client.fput_object(
                bucket_name=bucket,
                object_name=object_name,
                file_path=local_file)
            os.remove(local_file)
        else:
            print(f"File {local_file} does not exist, skipping upload.")


###############################################
with DAG(dag_id='Grab_NYC_Data_to_Minio',
         start_date=days_ago(1),
         schedule_interval=None,
         catchup=False,
         tags=['minio/read/write'],
         ) as dag:
    ###############################################
    # Create a task to call your processing function
    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )
    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )
###############################################

###############################################
# first upload the file, then read the other file.
t1 >> t2
###############################################