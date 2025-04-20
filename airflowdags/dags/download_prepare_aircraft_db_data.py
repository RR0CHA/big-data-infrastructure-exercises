import gzip
import json
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

bucket_name = Variable.get("s3_bucket_name")
s3_raw_key_aircraft_db_data = Variable.get("s3_raw_key_aircraft_db_data")
s3_prep_key_aircraft_db_data = Variable.get("s3_prep_key_aircraft_db_data")
url_aircraft_db_data   = Variable.get("url_aircraft_db_data")

def download_file():
    #url = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
    #bucket_name = "bdi-aircraft-01"
    #s3_key = "raw/aircraft_data/basic-ac-db.json.gz"

    response = requests.get(url_aircraft_db_data, stream=True)
    response.raise_for_status()

    buffer = BytesIO(response.content)

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_file_obj(buffer, key=s3_raw_key_aircraft_db_data, bucket_name=bucket_name, replace=True)

    print(f" Uploaded raw file to s3://{bucket_name}/{s3_raw_key_aircraft_db_data}")

def prepare_file():
    #aws_conn_id = "aws_default"
    #source_bucket = "source-bucket-name"
    #source_key = "path/to/your-file.json.gz"

    #dest_bucket = "dest-bucket-name"
    #dest_key = "path/to/converted-file.csv.gz"

    s3 = S3Hook(aws_conn_id="aws_default")

    # Download .json.gz from source S3
    obj = s3.get_key(s3_raw_key_aircraft_db_data, bucket_name=bucket_name)
    gzipped_data = obj.get()["Body"].read()

    # Decompress
    with gzip.GzipFile(fileobj=BytesIO(gzipped_data), mode='rb') as gz:
        lines = gz.read().decode('utf-8').splitlines()

    # Parse JSON lines
    json_records = [json.loads(line) for line in lines]

    # Convert to DataFrame
    df = pd.DataFrame(json_records)

    # Convert to CSV and compress to .csv.gz
    csv_buffer = BytesIO()
    with gzip.GzipFile(fileobj=csv_buffer, mode='wb') as gz_out:
        csv_text = df.to_csv(index=False)
        gz_out.write(csv_text.encode('utf-8'))

    csv_buffer.seek(0)

    # Upload compressed CSV to destination S3 bucket
    s3.load_file_obj(
        file_obj=csv_buffer,
        key=s3_prep_key_aircraft_db_data,
        bucket_name=bucket_name,
        replace=True
    )

    print(f"Uploaded prepared file to s3://{bucket_name}/{s3_prep_key_aircraft_db_data}")

def upsert_csv_to_postgres():
    # Download CSV.GZ from S3
    s3 = S3Hook(aws_conn_id="aws_default")
    obj = s3.get_key(s3_prep_key_aircraft_db_data, bucket_name=bucket_name)
    buffer = BytesIO(obj.get()['Body'].read())

    # Read gzip and load into DataFrame
    with gzip.GzipFile(fileobj=buffer) as gz:
        df = pd.read_csv(gz)

    pg_conn = connect_to_rds()
    conn = BaseHook.get_connection("rds_postgres")
    extras = conn.extra_dejson
    db_schema = extras.get("postgres_schema") #aircraft
    db_table = extras.get("postgres_table_aircraft_db_data")

    # SQL statements
    SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {db_schema};"

    TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.{db_table} (
        icao TEXT PRIMARY KEY,
        reg TEXT,
        icaotype TEXT,
        year TEXT,
        manufacturer TEXT,
        model TEXT,
        ownop TEXT,
        faa_pia BOOLEAN,
        faa_ladd BOOLEAN,
        short_type TEXT,
        mil BOOLEAN
    );
    """

    # Create a cursor object
    cur = pg_conn.cursor()

    # Ensure schema if not exists
    cur.execute(SCHEMA_SQL)

    # Create table  if not exists
    cur.execute(TABLE_SQL)

    # Prepare UPSERT query
    columns = list(df.columns)
    pkey = "icao"
    updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col != pkey])
    insert_query = f"""
        INSERT INTO {db_schema}.{db_table} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({pkey}) DO UPDATE SET {updates}
    """

    psycopg2.extras.execute_values(
        cur, insert_query, df.values.tolist(), template=None, page_size=1000
    )

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


# Define the DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="download_prepare_aircraft_db_data",
    default_args=default_args,
    description="Download aircraft DB (gzipped JSON), upload to S3, save data to RDS",
    schedule_interval=None,  # or "@weekly", or None for manual only
    start_date=datetime(2024, 4, 15),
    catchup=False,
    tags=["aircraft", "s3", "external_data"],
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
