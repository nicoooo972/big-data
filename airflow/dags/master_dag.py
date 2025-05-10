from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='master_etl_pipeline',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['master', 'etl'],
) as dag:
    
    # Trigger DAG 1 - Download data
    trigger_dag1 = TriggerDagRunOperator(
        task_id='trigger_download_data',
        trigger_dag_id='download_yellow_taxi_data',
        wait_for_completion=True,
        poke_interval=60
    )

    # Trigger DAG 2 - Load to DWH
    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_load_to_dwh',
        trigger_dag_id='load_data_to_datawarehouse',
        wait_for_completion=True,
        poke_interval=60
    )

    # Trigger DAG 3 - Load to Data Mart
    trigger_dag3 = TriggerDagRunOperator(
        task_id='trigger_load_to_mart',
        trigger_dag_id='migrate_datawarehouse_to_datamart',
        wait_for_completion=True,
        poke_interval=60
    )

    trigger_dag1 >> trigger_dag2 >> trigger_dag3 