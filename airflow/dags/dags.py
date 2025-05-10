# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_parquet(**kwargs):
    logger.info("Starting Parquet download task.")
    folder_path: str = '/tmp/data/raw'
    os.makedirs(folder_path, exist_ok=True)
    logger.info(f"Ensured download folder exists: {folder_path}")
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"
    start = pendulum.datetime(2025, 1, 1)
    for i in range(3):
        month = start.subtract(months=i).format('YYYY-MM')
        file_url = url + filename + '_' + month + extension
        local_file = os.path.join(folder_path, f"yellow_tripdata_{month}.parquet")
        try:
            logger.info(f"Attempting to download {file_url} to {local_file}")
            urllib.request.urlretrieve(file_url, local_file)
            logger.info(f"Successfully downloaded {file_url} to {local_file}")
        except urllib.error.URLError as e:
            logger.error(f"Failed to download {file_url}: {e}")


# Python Function
def upload_file(**kwargs):
    logger.info("Starting Minio upload task.")
    ###############################################
    # Upload generated file to Minio

    try:
        client = Minio(
            "minio:9000",
            secure=False,
            access_key="minio",
            secret_key="minio123"
        )
        logger.info("Minio client initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize Minio client: {e}")
        raise

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
            try:
                logger.info(f"Attempting to upload {local_file} to Minio bucket {bucket} as {object_name}")
                client.fput_object(
                    bucket_name=bucket,
                    object_name=object_name,
                    file_path=local_file)
                logger.info(f"Successfully uploaded {local_file} to {bucket}/{object_name}")
                os.remove(local_file)
                logger.info(f"Removed local file {local_file} after upload.")
            except S3Error as e:
                logger.error(f"Minio S3Error while uploading {local_file}: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while uploading {local_file}: {e}")
        else:
            logger.warning(f"File {local_file} does not exist, skipping upload.")
    logger.info("Finished Minio upload task.")


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