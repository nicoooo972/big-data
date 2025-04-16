from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sqlalchemy
import pandas as pd
import os

# Connexions
SRC_CONN = 'postgresql://postgres:admin@data-warehouse:5432/taxi'
DST_CONN = 'postgresql://postgres:admin@data-mart:5432/taxi_olap'


def migrate_dim_vendor(**kwargs):
    src = sqlalchemy.create_engine(SRC_CONN)
    dst = sqlalchemy.create_engine(DST_CONN)
    df = pd.read_sql('SELECT DISTINCT vendorid as vendor_id FROM yellow_tripdata WHERE vendorid IS NOT NULL', src)
    df.columns = df.columns.str.lower()
    existing = pd.read_sql('SELECT vendor_id FROM dim_vendor', dst)
    df = df[~df['vendor_id'].isin(existing['vendor_id'])]
    if not df.empty:
        df.to_sql('dim_vendor', dst, if_exists='append', index=False, method='multi')


def migrate_dim_rate_code(**kwargs):
    src = sqlalchemy.create_engine(SRC_CONN)
    dst = sqlalchemy.create_engine(DST_CONN)
    df = pd.read_sql('SELECT DISTINCT ratecodeid as rate_code_id FROM yellow_tripdata WHERE ratecodeid IS NOT NULL', src)
    df.columns = df.columns.str.lower()
    existing = pd.read_sql('SELECT rate_code_id FROM dim_rate_code', dst)
    df = df[~df['rate_code_id'].isin(existing['rate_code_id'])]
    if not df.empty:
        df.to_sql('dim_rate_code', dst, if_exists='append', index=False, method='multi')


def migrate_dim_payment_type(**kwargs):
    src = sqlalchemy.create_engine(SRC_CONN)
    dst = sqlalchemy.create_engine(DST_CONN)
    df = pd.read_sql('SELECT DISTINCT payment_type as payment_type_id FROM yellow_tripdata WHERE payment_type IS NOT NULL', src)
    df.columns = df.columns.str.lower()
    existing = pd.read_sql('SELECT payment_type_id FROM dim_payment_type', dst)
    df = df[~df['payment_type_id'].isin(existing['payment_type_id'])]
    if not df.empty:
        df.to_sql('dim_payment_type', dst, if_exists='append', index=False, method='multi')


def migrate_dim_location(**kwargs):
    src = sqlalchemy.create_engine(SRC_CONN)
    dst = sqlalchemy.create_engine(DST_CONN)
    df1 = pd.read_sql('SELECT DISTINCT pulocationid as location_id FROM yellow_tripdata WHERE pulocationid IS NOT NULL', src)
    df2 = pd.read_sql('SELECT DISTINCT dolocationid as location_id FROM yellow_tripdata WHERE dolocationid IS NOT NULL', src)
    df = pd.concat([df1, df2]).drop_duplicates()
    df.columns = df.columns.str.lower()
    existing = pd.read_sql('SELECT location_id FROM dim_location', dst)
    df = df[~df['location_id'].isin(existing['location_id'])]
    if not df.empty:
        df.to_sql('dim_location', dst, if_exists='append', index=False, method='multi')


def migrate_dim_date(**kwargs):
    src = sqlalchemy.create_engine(SRC_CONN)
    dst = sqlalchemy.create_engine(DST_CONN)
    df1 = pd.read_sql('SELECT DISTINCT DATE(tpep_pickup_datetime) as datum FROM yellow_tripdata WHERE tpep_pickup_datetime IS NOT NULL', src)
    df2 = pd.read_sql('SELECT DISTINCT DATE(tpep_dropoff_datetime) as datum FROM yellow_tripdata WHERE tpep_dropoff_datetime IS NOT NULL', src)
    df = pd.concat([df1, df2]).drop_duplicates()
    df['year'] = pd.to_datetime(df['datum']).dt.year
    df['month'] = pd.to_datetime(df['datum']).dt.month
    df['day'] = pd.to_datetime(df['datum']).dt.day
    df['day_of_week'] = pd.to_datetime(df['datum']).dt.isocalendar().day
    df['day_name'] = pd.to_datetime(df['datum']).dt.day_name()
    df['month_name'] = pd.to_datetime(df['datum']).dt.month_name()
    df['quarter'] = pd.to_datetime(df['datum']).dt.quarter
    df['is_weekend'] = df['day_of_week'].isin([6,7])
    df = df.rename(columns={'datum': 'full_date'})
    df.columns = df.columns.str.lower()
    existing = pd.read_sql('SELECT full_date FROM dim_date', dst)
    df = df[~df['full_date'].isin(existing['full_date'])]
    if not df.empty:
        df.to_sql('dim_date', dst, if_exists='append', index=False, method='multi')


def migrate_fact_trips(**kwargs):
    src = sqlalchemy.create_engine(SRC_CONN)
    dst = sqlalchemy.create_engine(DST_CONN)

    # Load source data
    df = pd.read_sql('SELECT * FROM yellow_tripdata', src)
    df.columns = df.columns.str.lower()

    # Load dimension tables
    dim_vendor = pd.read_sql('SELECT vendor_key, vendor_id FROM dim_vendor', dst)
    dim_rate_code = pd.read_sql('SELECT rate_code_key, rate_code_id FROM dim_rate_code', dst)
    dim_payment_type = pd.read_sql('SELECT payment_type_key, payment_type_id FROM dim_payment_type', dst)
    dim_location = pd.read_sql('SELECT location_key, location_id FROM dim_location', dst)
    dim_date = pd.read_sql('SELECT date_key, full_date FROM dim_date', dst)

    # Merge for vendor
    df = df.merge(dim_vendor, left_on='vendorid', right_on='vendor_id', how='left')
    # Merge for rate code
    df = df.merge(dim_rate_code, left_on='ratecodeid', right_on='rate_code_id', how='left')
    # Merge for payment type
    df = df.merge(dim_payment_type, left_on='payment_type', right_on='payment_type_id', how='left')
    # Merge for pickup location
    df = df.merge(dim_location, left_on='pulocationid', right_on='location_id', how='left', suffixes=('', '_pickup'))
    df = df.rename(columns={'location_key': 'pickup_location_key'})
    # Merge for dropoff location
    df = df.merge(dim_location, left_on='dolocationid', right_on='location_id', how='left', suffixes=('', '_dropoff'))
    df = df.rename(columns={'location_key': 'dropoff_location_key'})
    # Merge for pickup date
    df = df.merge(dim_date, left_on=df['tpep_pickup_datetime'].dt.date, right_on='full_date', how='left', suffixes=('', '_pickup_date'))
    df = df.rename(columns={'date_key': 'pickup_date_key'})
    # Merge for dropoff date
    df = df.merge(dim_date, left_on=df['tpep_dropoff_datetime'].dt.date, right_on='full_date', how='left', suffixes=('', '_dropoff_date'))
    df = df.rename(columns={'date_key': 'dropoff_date_key'})

    # Calcul de la durée
    df['trip_duration'] = df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']

    # Colonnes finales à insérer
    fact_cols = [
        'vendor_key',
        'pickup_date_key',
        'dropoff_date_key',
        'pickup_location_key',
        'dropoff_location_key',
        'rate_code_key',
        'payment_type_key',
        'store_and_fwd_flag',
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'congestion_surcharge',
        'Airport_fee',
        'trip_duration'
    ]
    df = df.loc[:,~df.columns.duplicated()]
    df[fact_cols].to_sql('fact_trips', dst, if_exists='append', index=False, method='multi')


def truncate_source_table(**kwargs):
    src = sqlalchemy.create_engine(SRC_CONN)
    with src.connect() as conn:
        conn.execute(sqlalchemy.text('TRUNCATE TABLE yellow_tripdata'))


with DAG(
    dag_id='migrate_datawarehouse_to_datamart',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['migration', 'postgres'],
) as dag:
    t1 = PythonOperator(
        task_id='migrate_dim_vendor',
        python_callable=migrate_dim_vendor
    )
    t2 = PythonOperator(
        task_id='migrate_dim_rate_code',
        python_callable=migrate_dim_rate_code
    )
    t3 = PythonOperator(
        task_id='migrate_dim_payment_type',
        python_callable=migrate_dim_payment_type
    )
    t4 = PythonOperator(
        task_id='migrate_dim_location',
        python_callable=migrate_dim_location
    )
    t5 = PythonOperator(
        task_id='migrate_dim_date',
        python_callable=migrate_dim_date
    )
    t6 = PythonOperator(
        task_id='migrate_fact_trips',
        python_callable=migrate_fact_trips
    )
    t7 = PythonOperator(
        task_id='truncate_source_table',
        python_callable=truncate_source_table
    )
    [t1, t2, t3, t4, t5] >> t6 >> t7 