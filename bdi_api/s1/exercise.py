import csv
import gzip
import json
import os
import shutil
from typing import Annotated
from urllib.request import Request, urlopen

import pandas as pd

#from fastapi import FastAPI, HTTPException, Query
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    # TODO Implement download

    """
    Fetch a list of file URLs from an external source and download them.
    The number of files downloaded is limited by the 'limit' query parameter.
    """

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
        shutil.rmtree(download_dir)
    except FileNotFoundError:
        #dir does not exist - no need to delete
        pass

    os.makedirs(download_dir, exist_ok=True)


    # Download files
    downloaded_files = []
    for url in file_urls:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Extract filename and save locally
            filename = os.path.basename(url)
            save_path = os.path.join(download_dir, f"{filename}")
            with open(save_path, "wb") as file:
                shutil.copyfileobj(response.raw, file)

            downloaded_files.append(save_path)
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Failed to download: {str(e)}") from e

    return "OK"



@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO

    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    # Clear previous prepared folder
    try:
        shutil.rmtree(prepared_dir)
    except FileNotFoundError:
        #dir does not exist - no need to delete
        pass

    os.makedirs(prepared_dir, exist_ok=True)

    #check if download_dir exist
    try:
        files = os.listdir(download_dir)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Download dir does exist.") from e


    # Iterate over all files in the folder
    for file_name in files:
        if file_name.endswith(".gz"):
            gz_file_path = os.path.join(download_dir, file_name)

            try:
                with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
                    # Parse the JSON content
                    json_content = json.load(gz_file)
                    # Extract the "data" field
                    now_timestamp = json_content.get("now")
                    csv_file_name = str(now_timestamp) + ".csv"  # Generate CSV file name
                    csv_file_path = os.path.join(prepared_dir, csv_file_name)

                    data = json_content.get("aircraft", [])

                    if data:
                        # Collect all unique field names from the entire data list
                        fieldnames = set()
                        for item in data:
                            fieldnames.update(item.keys())
                        fieldnames = sorted(fieldnames)  # Sort fieldnames to maintain order

                        with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                            writer.writeheader()

                            for item in data:
                                # Write each item, ensuring missing fields are handled
                                writer.writerow({field: item.get(field, None) for field in fieldnames})

                    else:
                        raise HTTPException(status_code=404, detail="No data found in file")
            except Exception as e:
                raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    all_files = [os.path.join(prepared_dir, f) for f in os.listdir(prepared_dir) if f.endswith('.csv')]
    dataframes = [pd.read_csv(file) for file in all_files]
    data = pd.concat(dataframes, ignore_index=True)

    if len(data) == 0:
        raise HTTPException(status_code=404, detail="No data found in files.")

    data = data[['hex','r','t']]

    # remove rows with null values in at least one column
    data = data.dropna()

    # remove duplicated rows
    data = data.drop_duplicates()

    # set fields per expected result
    data.rename(columns={'hex': 'icao', 'r': 'registration', 't': 'type'}, inplace=True)

    # sort by icao ascending
    data = data.sort_values(by=data.columns[0])

    # Implement pagination
    start_idx = page * num_results
    end_idx = start_idx + num_results
    paginated_data = data.iloc[start_idx:end_idx]

    # Convert the result to a list of dictionaries for JSON response
    result = paginated_data.to_dict(orient="records")

    return result


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    all_files = [f for f in os.listdir(prepared_dir) if f.endswith('.csv')]
    dataframes = []
    for file in all_files:
        timestamp = file.partition('.csv')[0]
        df = pd.read_csv(os.path.join(prepared_dir, file))
        df['timestamp_file'] = float(timestamp)
        dataframes.append(df)

    data = pd.concat(dataframes, ignore_index=True)

    data = data[['hex', 'timestamp_file', 'lat', 'lon', 'seen', 'seen_pos']]

    #Assuming: seen_pos gives how long ago (in seconds before “now”,
    # where "now" means the timestamp of the file) the position was last updated
    data['timestamp'] = data['timestamp_file'] - data['seen_pos']

    data = data[data['hex'] == icao]

    data = data[['timestamp', 'lat', 'lon']]

    # sort by icao ascending
    data = data.sort_values(by=data.columns[0])

    # Implement pagination
    start_idx = page * num_results
    end_idx = start_idx + num_results
    paginated_data = data.iloc[start_idx:end_idx]

    # Convert the result to a list of dictionaries for JSON response
    result = paginated_data.to_dict(orient="records")

    return result


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    all_files = [os.path.join(prepared_dir, f) for f in os.listdir(prepared_dir) if f.endswith('.csv')]
    dataframes = [pd.read_csv(file) for file in all_files]
    data = pd.concat(dataframes, ignore_index=True)

    data = data[['hex','alt_baro','gs','emergency']]

    data = data[data['hex'] == icao]

    result = {}

    #calculate max_altitude_baro
    max_alt_baro = data[data['alt_baro'] != "ground"]['alt_baro'].astype(float).max()
    result['max_altitude_baro'] = max_alt_baro

    #calculate max_ground_speed
    max_gs = data['gs'].max()
    result['max_ground_speed'] = max_gs

    #calculate had_emergency
    had_emergency = not data['emergency'].apply(lambda x: pd.isnull(x) or str(x).strip().lower() == 'none').all()

    result['had_emergency'] = str(had_emergency)

    return result


@s1.get("/aircraft/{icao}/stats2")
def get_aircraft_stats2(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns a list of statistics about the aircraft

    * all altitude_baro
    * all ground_speed
    * all emergency
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    all_files = [os.path.join(prepared_dir, f) for f in os.listdir(prepared_dir) if f.endswith('.csv')]
    dataframes = [pd.read_csv(file) for file in all_files]
    data = pd.concat(dataframes, ignore_index=True)

    data = data[['hex','alt_baro','gs','emergency']]

    data = data[data['hex'] == icao]

    # Implement pagination
    start_idx = page * num_results
    end_idx = start_idx + num_results
    paginated_data = data.iloc[start_idx:end_idx]

    # Convert the result to a list of dictionaries for JSON response
    result = paginated_data.to_dict(orient="records")

    return result
