from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sqlalchemy
import polars as pl
import os

# Connexions
SRC_CONN = 'postgresql://postgres:admin@data-warehouse:5432/taxi'
DST_CONN = 'postgresql://postgres:admin@data-mart:5432/taxi_olap'


def migrate_dim_vendor(**kwargs):
    query = 'SELECT DISTINCT vendorid as vendor_id FROM yellow_tripdata WHERE vendorid IS NOT NULL'
    df = pl.read_database_uri(query=query, uri=SRC_CONN)
    
    existing_query = 'SELECT vendor_id FROM dim_vendor'
    try:
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('vendor_id').is_in(existing['vendor_id']))
    except Exception:
         pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            df.write_database(table_name='dim_vendor', connection=engine, if_table_exists='append')
        finally:
            engine.dispose()


def migrate_dim_rate_code(**kwargs):
    query = 'SELECT DISTINCT ratecodeid as rate_code_id FROM yellow_tripdata WHERE ratecodeid IS NOT NULL'
    df = pl.read_database_uri(query=query, uri=SRC_CONN)
    
    existing_query = 'SELECT rate_code_id FROM dim_rate_code'
    try:
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('rate_code_id').is_in(existing['rate_code_id']))
    except Exception:
         pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            df.write_database(table_name='dim_rate_code', connection=engine, if_table_exists='append')
        finally:
            engine.dispose()


def migrate_dim_payment_type(**kwargs):
    query = 'SELECT DISTINCT payment_type as payment_type_id FROM yellow_tripdata WHERE payment_type IS NOT NULL'
    df = pl.read_database_uri(query=query, uri=SRC_CONN)
    
    existing_query = 'SELECT payment_type_id FROM dim_payment_type'
    try:
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('payment_type_id').is_in(existing['payment_type_id']))
    except Exception:
        pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            df.write_database(table_name='dim_payment_type', connection=engine, if_table_exists='append')
        finally:
            engine.dispose()


def migrate_dim_location(**kwargs):
    query1 = 'SELECT DISTINCT pulocationid as location_id FROM yellow_tripdata WHERE pulocationid IS NOT NULL'
    query2 = 'SELECT DISTINCT dolocationid as location_id FROM yellow_tripdata WHERE dolocationid IS NOT NULL'
    df1 = pl.read_database_uri(query=query1, uri=SRC_CONN)
    df2 = pl.read_database_uri(query=query2, uri=SRC_CONN)
    df = pl.concat([df1, df2]).unique(subset=['location_id'])
    
    existing_query = 'SELECT location_id FROM dim_location'
    try:
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN)
        df = df.filter(~pl.col('location_id').is_in(existing['location_id']))
    except Exception:
        pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            df.write_database(table_name='dim_location', connection=engine, if_table_exists='append')
        finally:
            engine.dispose()


def migrate_dim_date(**kwargs):
    query1 = 'SELECT DISTINCT CAST(tpep_pickup_datetime AS DATE) as full_date FROM yellow_tripdata WHERE tpep_pickup_datetime IS NOT NULL'
    query2 = 'SELECT DISTINCT CAST(tpep_dropoff_datetime AS DATE) as full_date FROM yellow_tripdata WHERE tpep_dropoff_datetime IS NOT NULL'
    df1 = pl.read_database_uri(query=query1, uri=SRC_CONN)
    df2 = pl.read_database_uri(query=query2, uri=SRC_CONN)

    df = pl.concat([df1, df2]).unique(subset=['full_date'])

    df = df.with_columns([
        pl.col('full_date').dt.year().alias('year'),
        pl.col('full_date').dt.month().alias('month'),
        pl.col('full_date').dt.day().alias('day'),
        pl.col('full_date').dt.weekday().alias('day_of_week'),
        pl.col('full_date').dt.strftime('%A').alias('day_name'),
        pl.col('full_date').dt.strftime('%B').alias('month_name'),
        pl.col('full_date').dt.quarter().alias('quarter'),
    ]).with_columns(
        pl.col('day_of_week').is_in([6, 7]).alias('is_weekend')
    )

    existing_query = 'SELECT full_date FROM dim_date'
    try:
        existing = pl.read_database_uri(query=existing_query, uri=DST_CONN).with_columns(
            pl.col('full_date').cast(pl.Date)
        )
        df = df.filter(~pl.col('full_date').is_in(existing['full_date']))
    except Exception as e:
        print(f"Could not read existing dim_date or table is empty: {e}")
        pass

    if not df.is_empty():
        engine = sqlalchemy.create_engine(DST_CONN)
        try:
            df.write_database(table_name='dim_date', connection=engine, if_table_exists='append')
        finally:
            engine.dispose()


def migrate_fact_trips(**kwargs):
    df = pl.read_database_uri(query='SELECT * FROM yellow_tripdata', uri=SRC_CONN)
    df = df.rename({col: col.lower() for col in df.columns})

    dim_vendor = pl.read_database_uri(query='SELECT vendor_key, vendor_id FROM dim_vendor', uri=DST_CONN)
    dim_rate_code = pl.read_database_uri(query='SELECT rate_code_key, rate_code_id FROM dim_rate_code', uri=DST_CONN)
    dim_payment_type = pl.read_database_uri(query='SELECT payment_type_key, payment_type_id FROM dim_payment_type', uri=DST_CONN)
    dim_location = pl.read_database_uri(query='SELECT location_key, location_id FROM dim_location', uri=DST_CONN)
    dim_date = pl.read_database_uri(query='SELECT date_key, full_date FROM dim_date', uri=DST_CONN).with_columns(
        pl.col('full_date').cast(pl.Date)
    )

    df = df.with_columns([
        pl.col('tpep_pickup_datetime').cast(pl.Datetime).dt.date().alias('pickup_date'),
        pl.col('tpep_dropoff_datetime').cast(pl.Datetime).dt.date().alias('dropoff_date')
    ])

    df = df.join(dim_vendor, left_on='vendorid', right_on='vendor_id', how='left')
    df = df.join(dim_rate_code, left_on='ratecodeid', right_on='rate_code_id', how='left')
    df = df.join(dim_payment_type, left_on='payment_type', right_on='payment_type_id', how='left')
    df = df.join(dim_location.rename({'location_key': 'pickup_location_key'}), left_on='pulocationid', right_on='location_id', how='left')
    df = df.join(dim_location.rename({'location_key': 'dropoff_location_key'}), left_on='dolocationid', right_on='location_id', how='left')
    df = df.join(dim_date.rename({'date_key': 'pickup_date_key'}), left_on='pickup_date', right_on='full_date', how='left')
    df = df.join(dim_date.rename({'date_key': 'dropoff_date_key'}), left_on='dropoff_date', right_on='full_date', how='left')

    # Calculate duration (original calculation)
    df = df.with_columns(
        (pl.col('tpep_dropoff_datetime') - pl.col('tpep_pickup_datetime')).alias('trip_duration_temp')
    )

    # Convert duration to 'X seconds' string format for PostgreSQL INTERVAL type
    df = df.with_columns(
        (
            pl.col("trip_duration_temp").dt.total_seconds().cast(pl.Int64).cast(pl.String)
            + pl.lit(" seconds")
        ).alias("trip_duration")
    ).drop("trip_duration_temp")

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
        'trip_duration'
    ]

    if 'passenger_count' in df.columns:
         df = df.with_columns(pl.col('passenger_count').cast(pl.Float64, strict=False))
    else:
         df = df.with_columns(pl.lit(None).cast(pl.Float64).alias('passenger_count'))

    if 'airport_fee' not in df.columns:
         fact_cols.remove('Airport_fee')
    elif 'Airport_fee' in fact_cols:
         fact_cols[fact_cols.index('Airport_fee')] = 'airport_fee'

    df_final = df.select(fact_cols)

    engine = sqlalchemy.create_engine(DST_CONN)
    try:
        df_final.write_database(table_name='fact_trips', connection=engine, if_table_exists='append')
    finally:
        engine.dispose()


def truncate_source_table(**kwargs):
    engine = sqlalchemy.create_engine(SRC_CONN)
    try:
        with engine.connect() as conn:
            with conn.begin():
                 conn.execute(sqlalchemy.text('TRUNCATE TABLE yellow_tripdata'))
    finally:
        engine.dispose()


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