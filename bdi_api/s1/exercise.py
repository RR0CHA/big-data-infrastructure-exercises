
import gzip
import json
import os
import shutil
from typing import Annotated
from urllib.request import Request, urlopen

import pandas as pd
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

    columns = ['hex', 'alt_baro','emergency','gs','lat','lon','r','t','timestamp']


    # Iterate over all files in the folder
    for file_name in files:
        df = pd.DataFrame(columns=columns)

        if file_name.endswith(".gz"):
            gz_file_path = os.path.join(download_dir, file_name)

            try:
                with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
                    # Parse the JSON content
                    json_content = json.load(gz_file)
                    now_timestamp = json_content.get("now")

                    data = json_content.get("aircraft", [])

                    if data:
                        for item in data:

                            item.update({"timestamp": (now_timestamp-item.get('seen_pos', 0))})
                            item.update({"alt_baro": (0 if item.get('alt_baro') == 'ground' else \
                                                      item.get('alt_baro'))})
                            item.update({'emergency': (None if item.get('emergency') is None else \
                                                       item.get('emergency'))})
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
                            #group_sorted.to_csv(file_path, index=False)

                            if file_exists:
                                group_sorted.to_csv(file_path, mode='a', index=False, header=False)
                            else:
                                group_sorted.to_csv(file_path, index=False)


                    else:
                        raise HTTPException(status_code=404, detail="No data found in file")

            except Exception as e:
                raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e


    return "OK"


# @s1.post("/aircraft/prepare1")
# def prepare_data1() -> str:
#     """Prepare the data in the way you think it's better for the analysis.

#     * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
#     * documentation: https://www.adsbexchange.com/version-2-api-wip/
#         See "Trace File Fields" section

#     Think about the way you organize the information inside the folder
#     and the level of preprocessing you might need.

#     To manipulate the data use any library you feel comfortable with.
#     Just make sure to configure it in the `pyproject.toml` file
#     so it can be installed using `poetry update`.

#     TIP: always clean the prepared folder before writing again to avoid having old files.

#     Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
#     """
#     # TODO

#     download_dir = os.path.join(settings.raw_dir, "day=20231101")
#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101_1")

#     # Clear previous prepared folder
#     try:
#         shutil.rmtree(prepared_dir)
#     except FileNotFoundError:
#         #dir does not exist - no need to delete
#         pass

#     os.makedirs(prepared_dir, exist_ok=True)

#     #check if download_dir exist
#     try:
#         files = os.listdir(download_dir)
#     except FileNotFoundError as e:
#         raise HTTPException(status_code=404, detail="Download dir does exist.") from e


#     # Iterate over all files in the folder
#     for file_name in files:
#         if file_name.endswith(".gz"):
#             gz_file_path = os.path.join(download_dir, file_name)

#             try:
#                 with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
#                     # Parse the JSON content
#                     json_content = json.load(gz_file)
#                     # Extract the "data" field
#                     now_timestamp = json_content.get("now")
#                     csv_file_name = str(now_timestamp) + ".csv"  # Generate CSV file name
#                     csv_file_path = os.path.join(prepared_dir, csv_file_name)

#                     data = json_content.get("aircraft", [])

#                     if data:
#                         # Collect all unique field names from the entire data list
#                         fieldnames = set()
#                         for item in data:
#                             fieldnames.update(item.keys())
#                         fieldnames = sorted(fieldnames)  # Sort fieldnames to maintain order

#                         with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
#                             writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
#                             writer.writeheader()

#                             for item in data:
#                                 # Write each item, ensuring missing fields are handled
#                                 writer.writerow({field: item.get(field, None) for field in fieldnames})

#                     else:
#                         raise HTTPException(status_code=404, detail="No data found in file")
#             except Exception as e:
#                 raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

#     return "OK"


# @s1.post("/aircraft/prepare2")
# def prepare_data2() -> str:
#     """Prepare the data in the way you think it's better for the analysis.

#     * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
#     * documentation: https://www.adsbexchange.com/version-2-api-wip/
#         See "Trace File Fields" section

#     Think about the way you organize the information inside the folder
#     and the level of preprocessing you might need.

#     To manipulate the data use any library you feel comfortable with.
#     Just make sure to configure it in the `pyproject.toml` file
#     so it can be installed using `poetry update`.

#     TIP: always clean the prepared folder before writing again to avoid having old files.

#     Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
#     """
#     # TODO

#     download_dir = os.path.join(settings.raw_dir, "day=20231101")
#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101_2")

#     # Clear previous prepared folder
#     try:
#         shutil.rmtree(prepared_dir)
#     except FileNotFoundError:
#         #dir does not exist - no need to delete
#         pass

#     os.makedirs(prepared_dir, exist_ok=True)

#     #check if download_dir exist
#     try:
#         files = os.listdir(download_dir)
#     except FileNotFoundError as e:
#         raise HTTPException(status_code=404, detail="Download dir does exist.") from e

#     fieldnames = {'r','t', 'lat', 'lon', 'alt_baro','gs','emergency', 'timestamp'}
#     fieldnames = sorted(fieldnames)

#     # Iterate over all files in the folder
#     for file_name in files:
#         if file_name.endswith(".gz"):
#             gz_file_path = os.path.join(download_dir, file_name)

#             try:
#                 with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
#                     # Parse the JSON content
#                     json_content = json.load(gz_file)
#                     now_timestamp = json_content.get("now")

#                     data = json_content.get("aircraft", [])

#                     if data:

#                         for item in data:
#                             icao = item.get('hex')
#                             file_name = f"{icao}.csv"
#                             file_path = os.path.join(prepared_dir, file_name)
#                             file_exists = os.path.isfile(file_path)

#                             item.update({"timestamp": (now_timestamp-item.get('seen_pos', 0))})
#                             item.update({"alt_baro": (0 if item.get('alt_baro') == 'ground' \
#                                                       else item.get('alt_baro'))})

#                             with open(file_path, mode='a', newline='') as file:
#                                 writer = csv.DictWriter(file, fieldnames=fieldnames)

#                                 # If the file does not exist, add the header
#                                 if not file_exists:
#                                     writer.writeheader()

#                                 # Add the object details as a new row
#                                 writer.writerow({field: item.get(field, None) for field in fieldnames})

#                     else:
#                         raise HTTPException(status_code=404, detail="No data found in file")

#             except Exception as e:
#                 raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

#     return "OK"


# @s1.post("/aircraft/prepare3")
# def prepare_data3() -> str:
#     """Prepare the data in the way you think it's better for the analysis.

#     * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
#     * documentation: https://www.adsbexchange.com/version-2-api-wip/
#         See "Trace File Fields" section

#     Think about the way you organize the information inside the folder
#     and the level of preprocessing you might need.

#     To manipulate the data use any library you feel comfortable with.
#     Just make sure to configure it in the `pyproject.toml` file
#     so it can be installed using `poetry update`.

#     TIP: always clean the prepared folder before writing again to avoid having old files.

#     Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
#     """
#     # TODO

#     download_dir = os.path.join(settings.raw_dir, "day=20231101")
#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101_3")

#     # Clear previous prepared folder
#     try:
#         shutil.rmtree(prepared_dir)
#     except FileNotFoundError:
#         #dir does not exist - no need to delete
#         pass

#     os.makedirs(prepared_dir, exist_ok=True)

#     #check if download_dir exist
#     try:
#         files = os.listdir(download_dir)
#     except FileNotFoundError as e:
#         raise HTTPException(status_code=404, detail="Download dir does exist.") from e

#     fieldnames = {'r','t', 'lat', 'lon', 'alt_baro','gs','emergency', 'timestamp'}
#     fieldnames = sorted(fieldnames)
#     dfs = pd.DataFrame(columns=['icao', 'dataframe'])

#     # Iterate over all files in the folder
#     for file_name in files:
#         if file_name.endswith(".gz"):
#             gz_file_path = os.path.join(download_dir, file_name)

#             try:
#                 with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
#                     # Parse the JSON content
#                     json_content = json.load(gz_file)
#                     now_timestamp = json_content.get("now")

#                     data = json_content.get("aircraft", [])

#                     if data:
#                         for item in data:

#                             item.update({"timestamp": (now_timestamp-item.get('seen_pos', 0))})
#                             item.update({"alt_baro": (0 if item.get('alt_baro') == 'ground' \
#                                                       else item.get('alt_baro'))})
#                             icao = item.get('hex')

#                             new_row = pd.DataFrame({field: item.get(field, None) for field in fieldnames}, index=[0])

#                             filtered_dfs = dfs[dfs['icao'] == icao]['dataframe']

#                             if not filtered_dfs.empty:
#                                 existing_df = dfs[dfs['icao'] == icao]['dataframe'].iloc[0]
#                                 existing_df = pd.concat([existing_df, new_row], ignore_index=True)
#                                 dfs.loc[dfs['icao'] == icao, 'dataframe'] = [existing_df]
#                             else:
#                                 new_df = pd.DataFrame({'icao': [icao], 'dataframe': [new_row]})
#                                 dfs = pd.concat([dfs, new_df], ignore_index=True)


#                     else:
#                         raise HTTPException(status_code=404, detail="No data found in file")

#             except Exception as e:
#                 raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

#     for _, row in dfs.iterrows():
#         icao = row['icao']  # Get the ICAO code
#         df = row['dataframe']  # Get the dataframe stored in the 'dataframe' column
#         filename = f"{icao}.csv"
#         file_path = os.path.join(prepared_dir, filename)
#         df.to_csv(file_path, index=False)

#     return "OK"


# @s1.post("/aircraft/prepare4")
# def prepare_data4() -> str:
#     """Prepare the data in the way you think it's better for the analysis.

#     * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
#     * documentation: https://www.adsbexchange.com/version-2-api-wip/
#         See "Trace File Fields" section

#     Think about the way you organize the information inside the folder
#     and the level of preprocessing you might need.

#     To manipulate the data use any library you feel comfortable with.
#     Just make sure to configure it in the `pyproject.toml` file
#     so it can be installed using `poetry update`.

#     TIP: always clean the prepared folder before writing again to avoid having old files.

#     Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
#     """
#     # TODO

#     download_dir = os.path.join(settings.raw_dir, "day=20231101")
#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101_4")

#     # Clear previous prepared folder
#     try:
#         shutil.rmtree(prepared_dir)
#     except FileNotFoundError:
#         #dir does not exist - no need to delete
#         pass

#     os.makedirs(prepared_dir, exist_ok=True)

#     #check if download_dir exist
#     try:
#         files = os.listdir(download_dir)
#     except FileNotFoundError as e:
#         raise HTTPException(status_code=404, detail="Download dir does exist.") from e

#     columns = ['hex', 'alt_baro','emergency','gs','lat','lon','r','t','timestamp']
#     df = pd.DataFrame(columns=columns)

#     # Iterate over all files in the folder
#     for file_name in files:
#         if file_name.endswith(".gz"):
#             gz_file_path = os.path.join(download_dir, file_name)

#             try:
#                 with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
#                     # Parse the JSON content
#                     json_content = json.load(gz_file)
#                     now_timestamp = json_content.get("now")

#                     data = json_content.get("aircraft", [])

#                     if data:
#                         for item in data:

#                             item.update({"timestamp": (now_timestamp-item.get('seen_pos', 0))})
#                             item.update({"alt_baro": (0 if item.get('alt_baro') == 'ground' \
#                                                       else item.get('alt_baro'))})
#                             item.update({'emergency': (None if item.get('emergency') is None else \
#                                                        item.get('emergency'))})
#                             item.update({'gs': (None if item.get('gs') is None else item.get('gs'))})
#                             item.update({'r': (None if item.get('r') is None else item.get('r'))})
#                             item.update({'t': (None if item.get('t') is None else item.get('t'))})
#                             item.update({'lat': (None if item.get('lat') is None else item.get('lat'))})
#                             item.update({'lon': (None if item.get('lon') is None else item.get('lon'))})

#                             new_row = pd.DataFrame([item])

#                             new_row = new_row[['hex', 'alt_baro', 'emergency','gs','lat','lon','r','t','timestamp']]

#                             df.loc[len(df)] = new_row.iloc[0]


#                     else:
#                         raise HTTPException(status_code=404, detail="No data found in file")

#             except Exception as e:
#                 raise HTTPException(status_code=404, detail=f"Error processing file: {str(e)}") from e

#     for hex_value, group in df.groupby('hex'):
#         # Sort each group by 'timestamp'
#         group_sorted = group.sort_values(by='timestamp')

#         # Drop the 'hex' column before saving
#         group_sorted = group_sorted.drop(columns=['hex'])

#         # Define the file name based on the 'hex' value (don't include 'hex' in the CSV content)
#         filename = f"{hex_value}.csv"
#         file_path = os.path.join(prepared_dir, filename)

#         # Save the sorted group to a CSV file
#         group_sorted.to_csv(file_path, index=False)


#     return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    all_files = [os.path.join(prepared_dir, f) for f in os.listdir(prepared_dir) if f.endswith('.csv')]

    dataframes = []
    for file in all_files:
        dataframe = pd.read_csv(file)
        dataframe['icao'] = os.path.splitext(os.path.basename(file))[0]
        dataframes.append(dataframe)

    data = pd.concat(dataframes, ignore_index=True)

    if len(data) == 0:
        raise HTTPException(status_code=404, detail="No data found in files.")

    data = data[['icao','r','t']]

    # remove duplicated rows
    data = data.drop_duplicates()

    # set fields per expected result
    data.rename(columns={'r': 'registration', 't': 'type'}, inplace=True)

    # sort by icao ascending
    data = data.sort_values(by=data.columns[0])

    # Implement pagination
    start_idx = page * num_results
    end_idx = start_idx + num_results
    paginated_data = data.iloc[start_idx:end_idx]

    # Convert the result to a list of dictionaries for JSON response
    result = paginated_data.to_dict(orient="records")

    return result


# @s1.get("/aircraft1/")
# def list_aircraft1(num_results: int = 100, page: int = 0) -> list[dict]:
#     """List all the available aircraft, its registration and type ordered by
#     icao asc
#     """

#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
#     all_files = [os.path.join(prepared_dir, f) for f in os.listdir(prepared_dir) if f.endswith('.csv')]
#     dataframes = [pd.read_csv(file) for file in all_files]
#     data = pd.concat(dataframes, ignore_index=True)

#     if len(data) == 0:
#         raise HTTPException(status_code=404, detail="No data found in files.")

#     data = data[['hex','r','t']]

#     # remove duplicated rows
#     data = data.drop_duplicates()

#     # set fields per expected result
#     data.rename(columns={'hex': 'icao', 'r': 'registration', 't': 'type'}, inplace=True)

#     # sort by icao ascending
#     data = data.sort_values(by=data.columns[0])

#     # Implement pagination
#     start_idx = page * num_results
#     end_idx = start_idx + num_results
#     paginated_data = data.iloc[start_idx:end_idx]

#     # Convert the result to a list of dictionaries for JSON response
#     result = paginated_data.to_dict(orient="records")

#     return result


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_name = f"{icao}.csv"
    file_path = os.path.join(prepared_dir, file_name)
    file_exists = os.path.isfile(file_path)

    if file_exists:
        data = pd.read_csv(file_path)
    else:
        data = pd.DataFrame(columns=['timestamp', 'lat', 'lon'])
        #raise HTTPException(status_code=404, detail="icao not found")

    data = data[['timestamp', 'lat', 'lon']]

    # remove rows with null values in at least one column
    data = data.dropna()

    # sort by icao ascending
    data = data.sort_values(by=data.columns[0])

    # Implement pagination
    start_idx = page * num_results
    end_idx = start_idx + num_results
    paginated_data = data.iloc[start_idx:end_idx]

    # Convert the result to a list of dictionaries for JSON response
    result = paginated_data.to_dict(orient="records")

    return result


# @s1.get("/aircraft/{icao}/positions1")
# def get_aircraft_position1(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
#     """Returns all the known positions of an aircraft ordered by time (asc)
#     If an aircraft is not found, return an empty list.
#     """

#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
#     all_files = [f for f in os.listdir(prepared_dir) if f.endswith('.csv')]
#     dataframes = []
#     for file in all_files:
#         timestamp = file.partition('.csv')[0]
#         df = pd.read_csv(os.path.join(prepared_dir, file))
#         df['timestamp_file'] = float(timestamp)
#         dataframes.append(df)

#     data = pd.concat(dataframes, ignore_index=True)

#     data = data[['hex', 'timestamp_file', 'lat', 'lon', 'seen', 'seen_pos']]

#     #Assuming: seen_pos gives how long ago (in seconds before “now”,
#     # where "now" means the timestamp of the file) the position was last updated
#     data['timestamp'] = data['timestamp_file'] - data['seen_pos']

#     data = data[data['hex'] == icao]

#     data = data[['timestamp', 'lat', 'lon']]

#     # remove rows with null values in at least one column
#     data = data.dropna()

#     # sort by icao ascending
#     data = data.sort_values(by=data.columns[0])

#     # Implement pagination
#     start_idx = page * num_results
#     end_idx = start_idx + num_results
#     paginated_data = data.iloc[start_idx:end_idx]

#     # Convert the result to a list of dictionaries for JSON response
#     result = paginated_data.to_dict(orient="records")

#     return result


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_name = f"{icao}.csv"
    file_path = os.path.join(prepared_dir, file_name)
    file_exists = os.path.isfile(file_path)

    if file_exists:
        data = pd.read_csv(file_path)
    else:
        data = pd.DataFrame(columns=['alt_baro','gs','emergency'])
    #    raise HTTPException(status_code=404, detail="icao not found")

    data = data[['alt_baro','gs','emergency']]

    result = {}

    #calculate max_altitude_baro
    max_alt_baro = data['alt_baro'].astype(float).max()
    result['max_altitude_baro'] = max_alt_baro

    #calculate max_ground_speed
    max_gs = data['gs'].max()
    result['max_ground_speed'] = max_gs

    #calculate had_emergency
    had_emergency = not data['emergency'].apply(lambda x: pd.isnull(x) or str(x).strip().lower() == 'none').all()
    result['had_emergency'] = had_emergency

    return result



# @s1.get("/aircraft/{icao}/stats1")
# def get_aircraft_statistics1(icao: str) -> dict:
#     """Returns different statistics about the aircraft

#     * max_altitude_baro
#     * max_ground_speed
#     * had_emergency
#     """

#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
#     all_files = [os.path.join(prepared_dir, f) for f in os.listdir(prepared_dir) if f.endswith('.csv')]
#     dataframes = [pd.read_csv(file) for file in all_files]
#     data = pd.concat(dataframes, ignore_index=True)

#     data = data[['hex','alt_baro','gs','emergency']]

#     data = data[data['hex'] == icao]

#     result = {}

#     #calculate max_altitude_baro
#     max_alt_baro = data[data['alt_baro'] != "ground"]['alt_baro'].astype(float).max()
#     result['max_altitude_baro'] = max_alt_baro

#     #calculate max_ground_speed
#     max_gs = data['gs'].max()
#     result['max_ground_speed'] = max_gs

#     #calculate had_emergency
#     had_emergency = not data['emergency'].apply(lambda x: pd.isnull(x) or str(x).strip().lower() == 'none').all()

#     result['had_emergency'] = had_emergency

#     return result




# @s1.get("/aircraft/{icao}/stats2")
# def get_aircraft_stats2(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
#     """Returns a list of statistics about the aircraft

#     * all altitude_baro
#     * all ground_speed
#     * all emergency
#     """

#     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
#     all_files = [os.path.join(prepared_dir, f) for f in os.listdir(prepared_dir) if f.endswith('.csv')]
#     dataframes = [pd.read_csv(file) for file in all_files]
#     data = pd.concat(dataframes, ignore_index=True)

#     data = data[['hex','alt_baro','gs','emergency']]

#     data = data[data['hex'] == icao]

#     # Implement pagination
#     start_idx = page * num_results
#     end_idx = start_idx + num_results
#     paginated_data = data.iloc[start_idx:end_idx]

#     # Convert the result to a list of dictionaries for JSON response
#     result = paginated_data.to_dict(orient="records")

#     return result
