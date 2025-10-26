"""API calls to get data from EDINET.

Need to do
0) Enable pydantic to validate variables.
1) Add more apis.
2) Add loggers with HTTP status codes.
"""

import datetime
import os
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Define the path to the .env file
env_path = Path(__file__).resolve().parents[3] / "env" / ".env"

# Load environment variables from .env file
load_dotenv(dotenv_path=env_path)


def get_edinet_document_list(date) -> pd.DataFrame:
    """Get a list of EDINET documents submitted on a specific date.
    Reference:

    Args:
        date (datetime.date): The date for which to retrieve the document list.

    Returns:
        pd.DataFrame: A DataFrame containing the document list.
    """
    # Load the EDINET API token from environment variables
    edinet_api_token = os.getenv("EDINET_API_TOKEN")
    if not edinet_api_token:
        raise ValueError("EDINET_API_TOKEN is not set in environment variables.")

    # Format the date as YYYY-MM-DD
    if type(date) is datetime.datetime or type(date) is pd.Timestamp:
        date_str = date.strftime("%Y-%m-%d")
    else:
        date_str = date

    # Define the API endpoint and parameters
    url = f"{os.getenv("EDINET_API_ENDPOINT")}/documents.json"
    params = {
        "date": date_str,
        "type": 2,  # Type 2 for all documents
        "Subscription-Key": edinet_api_token,
    }

    # Make the GET request to the EDINET API
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Error fetching data from EDINET API: {response.status_code}")

    # Parse the JSON response
    data = response.json()

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data.get("results", []))

    return df


def get_edinet_document(doc_id):
    # Load the EDINET API token from environment variables
    edinet_api_token = os.getenv("EDINET_API_TOKEN")

    if not edinet_api_token:
        raise ValueError("EDINET_API_TOKEN is not set in environment variables.")

    # Define the API endpoint and parameters
    url = f"{os.getenv("EDINET_API_ENDPOINT")}/documents/{doc_id}"
    params = {
        "type": 1,  # Type 1
        "Subscription-Key": edinet_api_token,
    }

    # Make the GET request to the EDINET API
    response = requests.get(url, params=params)

    return response


def download_edinet_document(doc_id, folder_path):
    """Download EDINET XBRL file as a Zip file."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    response = get_edinet_document(doc_id=doc_id)

    if response.status_code == 200:
        with open(os.path.join(folder_path, f"{doc_id}.zip"), "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
    elif response.status_code != 200:
        raise Exception(f"Error fetching data from EDINET API: {response.status_code}")


def download_edinet_documents(doc_list, folder_path):
    """Download multiple EDINET XBRL files as Zip files."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    for _, doc in doc_list.iterrows():
        download_edinet_document(doc_id=doc["docID"], folder_path=folder_path)
        unzip_file(file_name=doc["docID"], folder_path=folder_path, remove_zip=True)
        time.sleep(1)


def unzip_file(file_name, folder_path, remove_zip=False):
    """Unzip EDINET XBRL Zip file."""
    if not os.path.exists(os.path.join(folder_path, file_name)):
        os.makedirs(os.path.join(folder_path, file_name))

    with zipfile.ZipFile(os.path.join(folder_path, f"{file_name}.zip"), "r") as zip_ref:
        zip_ref.extractall(os.path.join(folder_path, file_name))

    if remove_zip:
        os.remove(os.path.join(folder_path, f"{file_name}.zip"))
