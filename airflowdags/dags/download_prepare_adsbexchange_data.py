import gzip
import json
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from urllib.request import Request, urlopen

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values

# Config
date_urls = [
    "2023/11/01/", "2023/12/01/", "2024/01/01/", "2024/02/01/", "2024/03/01/",
    "2024/04/01/", "2024/05/01/", "2024/06/01/", "2024/07/01/", "2024/08/01/",
    "2024/09/01/", "2024/10/01/", "2024/11/01/"
]

#get from airflow variables
base_url = Variable.get("url_aircraft_adsbexchange")
bucket_name = Variable.get("s3_bucket_name")


def download_and_upload_raw_to_s3(date_path):

    full_url = base_url + date_path
    req = Request(url=full_url, headers={'user-agent': 'bdi_api/0.0.1'})
    response = urlopen(req)
    html = BeautifulSoup(response, features="lxml")
    elements = html.find_all(attrs={"class": "name"})

    file_limit_per_day = Variable.get("file_limit_per_day")

    if int(file_limit_per_day) > 100:
        file_limit_per_day = "100"  #Limit to 100 readsb-hist files for each day

    file_urls = [full_url + elements[i].get_text() for i in range(min(int(file_limit_per_day), len(elements)))]

    s3 = S3Hook(aws_conn_id="aws_default")  #get aws_conn_id from airflow connections
    day = date_path.replace("/", "")

    for file_url in file_urls:
        filename = file_url.split("/")[-1]
        s3_key = f"raw/day={day}/{filename}"

        # Check if the file already exists in S3
        if s3.check_for_key(s3_key, bucket_name):
            print(f"Skipping {s3_key}, already exists in S3.")
            continue  # Skip uploading if file exists

        try:
            # Download file content
            file_response = requests.get(file_url, stream=True)
            file_response.raise_for_status()

            compressed_buffer = BytesIO()
            with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as gz:
                gz.write(file_response.content)  # Ensure it's in bytes

            s3_client = s3.get_conn()
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=compressed_buffer.getvalue(),  # Get compressed binary content
                ContentEncoding="gzip"
            )

            print(f"Uploaded {s3_key}")
        except Exception as e:
            print(f"Failed to upload {file_url}: {e}")


def prepare_and_upload_prep_to_s3(date_path):

        s3 = S3Hook(aws_conn_id="aws_default")
        s3_client = s3.get_conn()
        day = date_path.replace("/", "")
        s3_prefix_path = "raw/day="+day

        # List the objects in the bucket
        objects = s3_client.list_objects(Bucket=bucket_name, Prefix=s3_prefix_path)
        columns = ['hex', 'alt_baro','emergency','gs','lat','lon','r','t','timestamp']

        # Check if there are any objects to delete
        if 'Contents' in objects:
            for obj in objects['Contents']:

                df = pd.DataFrame(columns=columns)
                file = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])

                new_key = str(obj['Key']).replace(".json.gz", ".csv.gz")
                new_key = new_key.replace("raw", "prepared")

                # Check if the file already exists in S3
                if s3.check_for_key(new_key, bucket_name):
                    print(f"Skipping {new_key}, already exists in S3.")
                    continue  # Skip uploading if file exists

                gzipped_body = file['Body'].read()

                with gzip.GzipFile(fileobj=BytesIO(gzipped_body)) as gz:
                    json_data = json.load(gz)

                now_timestamp = json_data.get("now")

                data = json_data.get("aircraft", [])

                if len(data) > 0:

                    for item in data:

                        item.update({"timestamp": (now_timestamp-item.get('seen_pos', 0))})
                        item.update({"alt_baro": (0 if item.get('alt_baro') == 'ground'
                                                else item.get('alt_baro'))})
                        item.update({'emergency': (None if item.get('emergency') is None else
                                                (None if item.get('emergency') == 'none'
                                                    else item.get('emergency')))})
                        item.update({'gs': (None if item.get('gs') is None else item.get('gs'))})
                        item.update({'r': (None if item.get('r') is None else item.get('r'))})
                        item.update({'t': (None if item.get('t') is None else item.get('t'))})
                        item.update({'lat': (None if item.get('lat') is None else item.get('lat'))})
                        item.update({'lon': (None if item.get('lon') is None else item.get('lon'))})

                        new_row = pd.DataFrame([item])

                        new_row = new_row[['hex', 'alt_baro', 'emergency','gs','lat','lon','r','t','timestamp']]

                        df.loc[len(df)] = new_row.iloc[0]


                    df["alt_baro"] = df["alt_baro"].astype(float)
                    df["gs"] = df["gs"].astype(float)
                    df["lat"] = df["lat"].astype(float)
                    df["lon"] = df["lon"].astype(float)
                    df["timestamp"] = df["timestamp"].astype(str)

                    df = df.rename(columns={'hex': 'icao', 'gs': 'ground_speed', 'lat': 'latitude',
                                            'lon': 'longitude', 'r': 'registration', 't': 'type'})


                # Save to CSV
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)

                # Compress to gzip
                compressed_csv = BytesIO()
                with gzip.GzipFile(fileobj=compressed_csv, mode='wb') as gz:
                    gz.write(csv_buffer.getvalue().encode('utf-8'))
                compressed_csv.seek(0)

                # Save back to S3 with updated key
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=new_key,
                    Body=compressed_csv.getvalue(),
                    ContentEncoding="gzip",
                    ContentType="text/csv"
                )

                print(f"Prepared and uploaded to {new_key}")


#get rds_postgres from airflow connections
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


def save_to_rds(date_path):

        s3 = S3Hook(aws_conn_id="aws_default")
        s3_client = s3.get_conn()
        day = date_path.replace("/", "")
        s3_prefix_path = "prepared/day="+day

        # List the objects in the bucket
        objects = s3_client.list_objects(Bucket=bucket_name, Prefix=s3_prefix_path)
        columns = ['icao', 'alt_baro','emergency','ground_speed',
                   'latitude','longitude','registration','type','timestamp']
        df_all = pd.DataFrame(columns=columns)

        # Check if there are any objects to delete
        if 'Contents' in objects:
            for obj in objects['Contents']:

                #df = pd.DataFrame(columns=columns)
                file = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                gzipped_body = file['Body'].read()

                with gzip.GzipFile(fileobj=BytesIO(gzipped_body)) as gz:
                    df = pd.read_csv(gz)

                df["alt_baro"] = df["alt_baro"].astype(float)
                df["ground_speed"] = df["ground_speed"].astype(float)
                df["latitude"] = df["latitude"].astype(float)
                df["longitude"] = df["longitude"].astype(float)
                df["timestamp"] = df["timestamp"].astype(str)

                #df_all.loc[len(df)] = df.iloc[0]
                df_all = pd.concat([df_all, df], ignore_index=True)
                print(f"Saved to rds file: {str(obj['Key'])}")

            pg_conn = connect_to_rds()
            conn = BaseHook.get_connection("rds_postgres")
            extras = conn.extra_dejson
            db_schema = extras.get("postgres_schema") #get rds_postgres extra in airflow connections
            db_table = extras.get("postgres_table_aircraft_data")

            # SQL statements
            SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {db_schema};"

            TABLE_SQL = f"""
            CREATE TABLE IF NOT EXISTS {db_schema}.{db_table} (
                icao TEXT,
                alt_baro FLOAT,
                emergency TEXT,
                ground_speed FLOAT,
                latitude FLOAT,
                longitude FLOAT,
                registration TEXT,
                type TEXT,
                timestamp TEXT,
                PRIMARY KEY (icao, timestamp)
            );
            """

            # Create a cursor object
            cur = pg_conn.cursor()

            # Ensure schema if not exists
            cur.execute(SCHEMA_SQL)

            # Create table  if not exists
            cur.execute(TABLE_SQL)

            records = [tuple(row) for row in df_all.itertuples(index=False, name=None)]
            #print(str(records))

            # SQL query to upsert data
            query = f"""
            INSERT INTO {db_schema}.{db_table} (
                icao, alt_baro, emergency, ground_speed, latitude,
                longitude, registration, type, timestamp
            )
            VALUES %s
            ON CONFLICT (icao, timestamp) DO UPDATE SET
                alt_baro = EXCLUDED.alt_baro,
                emergency = EXCLUDED.emergency,
                ground_speed = EXCLUDED.ground_speed,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                registration = EXCLUDED.registration,
                type = EXCLUDED.type;
                """

            # Execute batch insert
            execute_values(cur, query, records)

            pg_conn.commit()
            cur.close()
            pg_conn.close()



# DAG setup
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="download_prepare_adsbexchange_data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["adsb", "s3", "download", "download", "idempotency"],
) as dag:

    for date_path in date_urls:
        date_tag = date_path.replace("/", "_")

        with TaskGroup(group_id=f"group_{date_tag}") as tg:
            download_task = PythonOperator(
                task_id="download",
                python_callable=download_and_upload_raw_to_s3,
                op_args=[date_path],
            )

            prepare_task = PythonOperator(
                task_id="prepare",
                python_callable=prepare_and_upload_prep_to_s3,
                op_args=[date_path],
            )

            save_to_db_task = PythonOperator(
                task_id="save_to_db",
                python_callable=save_to_rds,
                op_args=[date_path],
            )

            download_task >> prepare_task >> save_to_db_task
