import json
from datetime import datetime, timedelta
from io import BytesIO, StringIO

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from psycopg2.extras import execute_values

bucket_name = Variable.get("s3_bucket_name")
s3_raw_key_aircraft_type_fuel = Variable.get("s3_raw_key_aircraft_type_fuel")
s3_prep_key_aircraft_type_fuel = Variable.get("s3_prep_key_aircraft_type_fuel")
url_aircraft_type_fuel  = Variable.get("url_aircraft_type_fuel")


def download_file():
    #url = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
    #bucket_name = "bdi-aircraft-01"
    #s3_key = "raw/aircraft_data/aircraft_type_fuel_consumption_rates.json"

    response = requests.get(url_aircraft_type_fuel)
    response.raise_for_status()

    buffer = BytesIO(response.content)

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_file_obj(
        file_obj=buffer,
        key=s3_raw_key_aircraft_type_fuel,
        bucket_name=bucket_name,
        replace=True
    )

    print(f"File aircraft_type_fuel_consumption_rates uploaded to s3://{bucket_name}/{s3_raw_key_aircraft_type_fuel}")

def prepare_file():

    #source_bucket = "source-bucket"
    #source_key = "aircraft_data.json"

    #dest_bucket = "dest-bucket"
    #dest_key = "processed_aircraft_data.csv"

    s3 = S3Hook(aws_conn_id="aws_default")

    # Download JSON file from source bucket
    file_obj = s3.get_key(s3_raw_key_aircraft_type_fuel, bucket_name=bucket_name)
    raw_json = file_obj.get()["Body"].read().decode("utf-8")

    # Parse and reshape JSON
    data = json.loads(raw_json)
    processed = [
        {
            "aircraft_type": aircraft_type,
            "aircraft_name": info.get("name"),
            "galph": info.get("galph"),
            "category": info.get("category")
        }
        for aircraft_type, info in data.items()
    ]

    df = pd.DataFrame(processed)

    # Save to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to destination bucket
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=s3_prep_key_aircraft_type_fuel,
        bucket_name=bucket_name,
        replace=True
    )

    print(f"CSV uploaded to s3://{bucket_name}/{s3_prep_key_aircraft_type_fuel}")


def upsert_csv_to_postgres():
    #aws_conn_id = "aws_default"
    #bucket_name = "dest-bucket"
    #key = "processed_aircraft_data.csv"

    s3 = S3Hook(aws_conn_id="aws_default")
    file_obj = s3.get_key(s3_prep_key_aircraft_type_fuel, bucket_name=bucket_name)
    content = file_obj.get()["Body"].read().decode("utf-8")

    df = pd.read_csv(StringIO(content))

    #pg_conn_info = BaseHook.get_connection("rds_postgres")
    pg_conn = connect_to_rds()
    conn = BaseHook.get_connection("rds_postgres")
    extras = conn.extra_dejson
    db_schema = extras.get("postgres_schema") #aircraft
    db_table = extras.get("postgres_table_aircraft_type_fuel")

    # SQL statements
    SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {db_schema};"

    TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.{db_table} (
        aircraft_type TEXT PRIMARY KEY,
        aircraft_name TEXT,
        galph INTEGER,
        category TEXT
    );
    """

    # Create a cursor object
    cur = pg_conn.cursor()

    # Ensure schema if not exists
    cur.execute(SCHEMA_SQL)

    # Create table  if not exists
    cur.execute(TABLE_SQL)

    query = f"""
        INSERT INTO {db_schema}.{db_table} (aircraft_type, aircraft_name, galph, category)
        VALUES %s
        ON CONFLICT (aircraft_type) DO UPDATE SET
            aircraft_name = EXCLUDED.aircraft_name,
            galph = EXCLUDED.galph,
            category = EXCLUDED.category;
    """

    records = df.values.tolist()
    execute_values(cur, query, records)

    pg_conn.commit()
    cur.close()
    pg_conn.close()

    print("Upsert to PostgreSQL completed.")


def connect_to_rds():
    conn = BaseHook.get_connection("rds_postgres")

    connection = psycopg2.connect(
        host=conn.host,
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port or 5432
    )

    return connection


# DAG configuration
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="download_prepare_aircraft_fuel_data",
    default_args=default_args,
    description="Download aircraft fuel consumption data, upload to S3 and save to RDS",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # or set to None to trigger manually
    catchup=False,
    tags=["aircraft", "fuel", "s3", "data_pipeline"],
) as dag:

    download_task = PythonOperator(
        task_id="download_and_upload_to_s3",
        python_callable=download_file,
    )

    prepare_task = PythonOperator(
        task_id="prepare_and_upload_to_s3",
        python_callable=prepare_file,
    )

    save_to_db_task = PythonOperator(
        task_id="save_data_to_rds",
        python_callable=upsert_csv_to_postgres,
    )

    download_task >> prepare_task >> save_to_db_task
