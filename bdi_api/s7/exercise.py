
import gzip
import json
import os
from io import BytesIO

import boto3
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, status
from psycopg2.extras import execute_values

from bdi_api.settings import DBCredentials, Settings  #, AWSCredentials

settings = Settings()
db_credentials = DBCredentials()
#aws_credentials = AWSCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

# Load environment variables from .env file
load_dotenv()

# Retrieve AWS credentials from environment variables
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")
#aws_region = os.getenv("AWS_REGION", "us-east-1")  # Default to us-east-1 if not provided

db_schema = os.getenv("BDI_DB_SCHEMA")
db_table = os.getenv("BDI_DB_TABLE")

# Create a session using the loaded credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    aws_session_token=aws_session_token
)


@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """
    #user = db_credentials.username
    # TODO

    # SQL statements
    CREATE_SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {db_schema};"

    DROP_TABLE_SQL = f"DROP TABLE IF EXISTS {db_schema}.{db_table};"

    CREATE_TABLE_SQL = f"""
    CREATE TABLE {db_schema}.{db_table} (
        icao TEXT,
        alt_baro FLOAT,
        emergency TEXT,
        ground_speed FLOAT,
        latitude FLOAT,
        longitude FLOAT,
        registration TEXT,
        type TEXT,
        timestamp TEXT
    );
    """

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=db_credentials.host,
            database=db_credentials.database,
            user=db_credentials.username,
            password=db_credentials.password,
            port=db_credentials.port
        )

        # Create a cursor object
        cur = conn.cursor()

        # Ensure schema exists
        cur.execute(CREATE_SCHEMA_SQL)

        # Drop table if it exists
        cur.execute(DROP_TABLE_SQL)

        # Create new table
        cur.execute(CREATE_TABLE_SQL)

        #print(f"Table '{db_schema}.{db_table}' has been successfully created.")

        s3_bucket = settings.s3_bucket
        s3_raw_prefix_path = "raw/day=20231101/"

        s3_client = boto3.client('s3')

        continuation_token = None

        while True:
            # If continuation token exists, use it to fetch the next page of results
            if continuation_token:
                response = s3_client.list_objects_v2(Bucket=s3_bucket,
                                                     Prefix=s3_raw_prefix_path, ContinuationToken=continuation_token)
            else:
                response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_raw_prefix_path)

            columns = ['hex', 'alt_baro','emergency','gs','lat','lon','r','t','timestamp']

            if 'Contents' in response:
                for obj in response['Contents']:

                    df = pd.DataFrame(columns=columns)

                    try:
                        # Parse the JSON content

                        response = s3_client.get_object(Bucket=s3_bucket, Key=obj['Key'])
                        compressed_body = response["Body"].read()
                        with gzip.GzipFile(fileobj=BytesIO(compressed_body), mode="rb") as gz:
                            decompressed_data = gz.read()  # Convert bytes to string

                        json_data = json.loads(decompressed_data)

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

                            records = [tuple(row) for row in df.itertuples(index=False, name=None)]

                            # SQL query to insert data
                            insert_query = f"""
                            INSERT INTO {db_schema}.{db_table} (
                                icao, alt_baro, emergency, ground_speed, latitude,
                                longitude, registration, type, timestamp
                            ) VALUES %s;
                            """

                            # Execute batch insert
                            execute_values(cur, insert_query, records)

                        else:
                            raise HTTPException(status_code=404, detail="No data found in file")

                    except Exception as e:
                        raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

            # Check if there are more objects (pagination)
            if response.get("IsTruncated"):  # If True, more objects exist
                continuation_token = response["NextContinuationToken"]
            else:
                break

        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

    return "OK"


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO

    offset = page * num_results

    query = f"""
    select distinct icao, registration, type from  {db_schema}.{db_table} order by icao asc LIMIT %s OFFSET %s;
    """

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=db_credentials.host,
            database=db_credentials.database,
            user=db_credentials.username,
            password=db_credentials.password,
            port=db_credentials.port
        )

        cursor = conn.cursor()
        cursor.execute(query, (num_results, offset))
        records = cursor.fetchall()

        #close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

    columns = ["icao", "registration", "type"]
    result = [dict(zip(columns, row)) for row in records]

    return result


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    offset = page * num_results

    query = f"""
    select timestamp, latitude, longitude from {db_schema}.{db_table}
    where icao = '{icao}' and timestamp IS NOT NULL and latitude IS NOT NULL and longitude IS NOT NULL
    order by timestamp asc LIMIT %s OFFSET %s;
    """

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=db_credentials.host,
            database=db_credentials.database,
            user=db_credentials.username,
            password=db_credentials.password,
            port=db_credentials.port
        )

        cursor = conn.cursor()
        cursor.execute(query, (num_results, offset))
        records = cursor.fetchall()

        #close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

    columns = ["timestamp", "lat", "lon"]
    result = [dict(zip(columns, row)) for row in records]

    return result


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO

    query = f"""
    select max(alt_baro) , max(ground_speed), case when max(emergency)
    is null then false else true end as had_emergency
    from {db_schema}.{db_table} where icao = '{icao}';
    """

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=db_credentials.host,
            database=db_credentials.database,
            user=db_credentials.username,
            password=db_credentials.password,
            port=db_credentials.port
        )

        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        first_row = records[0]

        #close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

    columns = ["max_altitude_baro", "max_ground_speed", "had_emergency"]
    result = dict(zip(columns, first_row))

    return result
