import gzip
import json
import os
import shutil
from io import BytesIO
from typing import Annotated
from urllib.request import Request, urlopen

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query

from bdi_api.settings import Settings

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials and session token from environment variables
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')  # Make sure to include this for temporary credentials

# Create a session using the loaded credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    aws_session_token=aws_session_token,  # Use the session token if you have it
    #region_name=aws_region
)

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    # TODO

    s3_client = boto3.client('s3')

    if file_limit <= 0:
        raise HTTPException(status_code=400, detail="Limit must be greater than 0")

    # Fetch file URLs from the external API
    try:

        req = Request(url=base_url,headers={'user-agent': 'bdi_api/0.0.1'})
        response = urlopen(req)
        html = BeautifulSoup(response, features="lxml")
        elements = html.find_all(attrs={"class": "name"})

        file_urls = []
        for i in range(file_limit): # Limit the number of files
            file_urls.append(base_url+elements[i].get_text())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch file URLs: {str(e)}") from e

    if not file_urls:
        raise HTTPException(status_code=404, detail="No files found to download.")


    # Clear previous downloads
    try:
        # List the objects in the bucket
        objects = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_prefix_path)

        # Check if there are any objects to delete
        if 'Contents' in objects:
            # Delete the objects in the bucket
            delete_objects = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_objects)


    except Exception as e:
        print(f"Error: {e}")


    # Download files
    for url in file_urls:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            filename = os.path.basename(url)

            file_content = response.content

            s3_file_path = s3_prefix_path+filename #'folder_name/your_file_name.ext'  # e.g., 'uploads/test_file.txt'

            compressed_buffer = BytesIO()
            with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as gz:
                gz.write(file_content)  # Ensure it's in bytes

            # Upload the file directly from the in-memory content (without saving to a local file)
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_file_path,
                Body=compressed_buffer.getvalue(),  # Get compressed binary content
                ContentEncoding="gzip"
            )
            #print(f"File successfully uploaded to {s3_bucket}/{s3_file_path}")

        except Exception as e:

            raise HTTPException(status_code=404, detail=f"Failed to download: {str(e)}") from e

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO

    # Clear previous prepared folder

    s3_bucket = settings.s3_bucket
    s3_raw_prefix_path = "raw/day=20231101/"
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    s3_client = boto3.client('s3')

    # Clear previous prepared folder
    try:
        shutil.rmtree(prepared_dir)
    except FileNotFoundError:
        #dir does not exist - no need to delete
        pass

    os.makedirs(prepared_dir, exist_ok=True)

    response = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_raw_prefix_path)

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
                        item.update({"alt_baro": (0 if item.get('alt_baro') == 'ground' else item.get('alt_baro'))})
                        item.update({'emergency': (None if item.get('emergency') is None else item.get('emergency'))})
                        item.update({'gs': (None if item.get('gs') is None else item.get('gs'))})
                        item.update({'r': (None if item.get('r') is None else item.get('r'))})
                        item.update({'t': (None if item.get('t') is None else item.get('t'))})
                        item.update({'lat': (None if item.get('lat') is None else item.get('lat'))})
                        item.update({'lon': (None if item.get('lon') is None else item.get('lon'))})

                        new_row = pd.DataFrame([item])

                        new_row = new_row[['hex', 'alt_baro', 'emergency','gs','lat','lon','r','t','timestamp']]

                        df.loc[len(df)] = new_row.iloc[0]


                    for hex_value, group in df.groupby('hex'):

                        # Drop the 'hex' column before saving
                        group_sorted = group.drop(columns=['hex'])

                        # Define the file name based on the 'hex' value (don't include 'hex' in the CSV content)
                        filename = f"{hex_value}.csv"
                        file_path = os.path.join(prepared_dir, filename)
                        file_exists = os.path.isfile(file_path)

                        #if file_exists:
                        #    existing_df = pd.read_csv(file_path)
                        #    group_sorted = pd.concat([group_sorted, existing_df], ignore_index=True)


                        # Sort each group by 'timestamp'
                        group_sorted = group_sorted.sort_values(by='timestamp')

                        # Save the sorted group to a CSV file
                        if file_exists:
                            group_sorted.to_csv(file_path, mode='a', index=False, header=False)
                        else:
                            group_sorted.to_csv(file_path, index=False)


                else:
                    raise HTTPException(status_code=404, detail="No data found in file")

            except Exception as e:
                raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

    else:
        print(f"No files found in S3 {s3_raw_prefix_path}.")


    return "OK"


# @s4.post("/aircraft/prepare1")
# def prepare_data1() -> str:
#     """Obtain the data from AWS s3 'download' and store it in the S3 'prepared' directory.

#     All the `/api/s1/aircraft/` endpoints should work as usual
#     """
#     # TODO

#     # Clear previous prepared folder

#     s3_bucket = settings.s3_bucket
#     s3_raw_prefix_path = "raw/day=20231101/"
#     s3_pre_prefix_path = "prepared/day=20231101/"
#     # TODO

#     s3_client = boto3.client('s3')

#     # Clear previous preparation
#     try:
#         # List the objects in the bucket
#         objects = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_pre_prefix_path)

#         # Check if there are any objects to delete
#         if 'Contents' in objects:
#             # Delete the objects in the bucket
#             delete_objects = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
#             s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_objects)

#     except Exception as e:
#         print(f"Error: {e}")

#     response = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_raw_prefix_path)

#     columns = ['hex', 'alt_baro','emergency','gs','lat','lon','r','t','timestamp']

#     #if 'Contents' in response:
#     for obj in response['Contents']:

#         df = pd.DataFrame(columns=columns)

#         try:
#             # Parse the JSON content
#             response = s3_client.get_object(Bucket=s3_bucket, Key=obj['Key'])
#             compressed_body = response["Body"].read()
#             with gzip.GzipFile(fileobj=BytesIO(compressed_body), mode="rb") as gz:
#                 decompressed_data = gz.read()  # Convert bytes to string

#             json_data = json.loads(decompressed_data)
#             now_timestamp = json_data.get("now")

#             data = json_data.get("aircraft", [])

#             #if len(data) > 0:

#             for item in data:

#                 item.update({"timestamp": (now_timestamp-item.get('seen_pos', 0))})
#                 item.update({"alt_baro": (0 if item.get('alt_baro') == 'ground' else item.get('alt_baro'))})
#                 item.update({'emergency': (None if item.get('emergency') is None else item.get('emergency'))})
#                 item.update({'gs': (None if item.get('gs') is None else item.get('gs'))})
#                 item.update({'r': (None if item.get('r') is None else item.get('r'))})
#                 item.update({'t': (None if item.get('t') is None else item.get('t'))})
#                 item.update({'lat': (None if item.get('lat') is None else item.get('lat'))})
#                 item.update({'lon': (None if item.get('lon') is None else item.get('lon'))})

#                 new_row = pd.DataFrame([item])

#                 new_row = new_row[['hex', 'alt_baro', 'emergency','gs','lat','lon','r','t','timestamp']]

#                 df.loc[len(df)] = new_row.iloc[0]


#             for hex_value, group in df.groupby('hex'):

#                 # Drop the 'hex' column before saving
#                 group_sorted = group.drop(columns=['hex'])

#                 # Define the file name based on the 'hex' value (don't include 'hex' in the CSV content)
#                 filename = f"{hex_value}.csv"

#                 try:
#                     response = s3_client.get_object(Bucket=s3_bucket, Key=(s3_pre_prefix_path+filename))
#                 except ClientError as e:
#                     if e.response['Error']['Code'] == 'NoSuchKey':
#                         #file does not exist
#                         pass
#                     else:
#                         print(f"An error occurred: {e}")

#                 if 'Contents' in response:
#                     existing_df = pd.read_csv(response['Body'])
#                     group_sorted = pd.concat([group_sorted, existing_df], ignore_index=True)

#                 # Sort each group by 'timestamp'
#                 group_sorted = group_sorted.sort_values(by='timestamp')

#                 csv_buffer = StringIO()
#                 group_sorted.to_csv(csv_buffer, index=False)

#                 # Save the sorted group to a CSV file
#                 s3_client.put_object(Bucket=s3_bucket, Key=(s3_pre_prefix_path+filename), \
#                                         Body=csv_buffer.getvalue())


#             #else:
#             #    raise HTTPException(status_code=404, detail="No data found in file")

#         except Exception as e:
#             raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

#     #else:
#     #    print(f"No files found in {s3_raw_prefix_path}.")


#     return "OK"
