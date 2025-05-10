from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from minio import Minio
import os
import pendulum
import polars as pl
import sqlalchemy
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_from_minio(**kwargs):
    logger.info("Starting Minio download task.")
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "spark"
    os.makedirs("/tmp/data/raw", exist_ok=True)
    downloaded_files = []
    logger.info(f"Listing objects in bucket: {bucket}")
    objects = list(client.list_objects(bucket, recursive=True))
    logger.info(f"Found {len(objects)} objects in bucket.")

    for obj in objects:
        if not obj.object_name.endswith('.parquet'):
            logger.info(f"Skipping non-parquet object: {obj.object_name}")
            continue
        local_file = os.path.join("/tmp/data/raw", obj.object_name)
        logger.info(f"Attempting to download {obj.object_name} to {local_file}")
        try:
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            client.fget_object(bucket, obj.object_name, local_file)
            downloaded_files.append(local_file)
            logger.info(f"Successfully downloaded {obj.object_name}")
        except Exception as e:
            logger.error(f"Failed to download {obj.object_name}: {e}")
    
    if not downloaded_files:
        logger.warning("No files were downloaded.")
    else:
        logger.info(f"Downloaded {len(downloaded_files)} files: {downloaded_files}")
    kwargs['ti'].xcom_push(key='local_files', value=downloaded_files)

def insert_into_postgres(**kwargs):
    logger.info("Starting PostgreSQL insert task.")
    local_files = kwargs['ti'].xcom_pull(key='local_files')
    if not local_files:
        logger.warning("No local files found to insert. Skipping database insertion.")
        return

    connection_uri = 'postgresql://postgres:admin@data-warehouse:5432/taxi'
    logger.info(f"Connecting to PostgreSQL: {connection_uri.split('@')[-1]}")
    
    engine = None
    try:
        engine = sqlalchemy.create_engine(connection_uri)
        logger.info(f"Processing {len(local_files)} files for PostgreSQL insertion.")
        for i, local_file in enumerate(local_files):
            logger.info(f"Processing file {i+1}/{len(local_files)}: {local_file}")
            try:
                df = pl.read_parquet(local_file)
                df = df.rename({col: col.lower() for col in df.columns})

                # Columns to drop if they exist
                cols_to_drop = ['airport_fee', 'cbd_congestion_fee']
                existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]
                if existing_cols_to_drop:
                    df = df.drop(existing_cols_to_drop)
                    logger.info(f"Dropped columns: {existing_cols_to_drop} from DataFrame from file {local_file}")

                logger.info(f"Writing {len(df)} rows from {local_file} to table 'yellow_tripdata'")
                df.write_database(table_name='yellow_tripdata', connection=engine, if_table_exists='append')
                logger.info(f"Successfully wrote {local_file} to PostgreSQL.")
                os.remove(local_file)
                logger.info(f"Removed local file: {local_file}")
            except Exception as e:
                logger.error(f"Error processing file {local_file}: {e}")

    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL or other critical error: {e}")
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("PostgreSQL engine disposed.")
    logger.info("PostgreSQL insert task finished.")

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
