"""API calls to get data from EDINET.

Referenece: https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140206.pdf

Need to do
1) Add more loggers with HTTP status codes.
2) Add more error handling.
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
    """Get a list of EDINET documents submitted on a specific date."""
    # Load the EDINET API token from environment variables
    edinet_api_token = os.getenv("EDINET_API_TOKEN")
    if not edinet_api_token:
        raise ValueError("EDINET_API_TOKEN is not set in environment variables.")

    # Format the date as YYYY-MM-DD and a string value
    if type(date) is datetime.datetime or type(date) is pd.Timestamp:
        date_str = date.strftime("%Y-%m-%d")
    else:
        date_str = date

    # Define the API endpoint and parameters
    url = f"{os.getenv("EDINET_API_ENDPOINT")}/documents.json"
    params = {
        "date": date_str,
        "type": 2,  # Type 2 for all documents, need to be a variable?
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


def get_edinet_document(doc_id) -> requests.Response:
    """Get EDINET document by document ID."""
    # Load the EDINET API token from environment variables
    edinet_api_token = os.getenv("EDINET_API_TOKEN")

    if not edinet_api_token:
        raise ValueError("EDINET_API_TOKEN is not set in environment variables.")

    # Define the API endpoint and parameters
    url = f"{os.getenv("EDINET_API_ENDPOINT")}/documents/{doc_id}"
    params = {
        "type": 1,  # Type 1, Need to be a variable?
        "Subscription-Key": edinet_api_token,
    }

    # Make the GET request to the EDINET API
    response = requests.get(url, params=params)

    return response


def download_edinet_document(doc_id, folder_path) -> None:
    """Download EDINET XBRL file as a Zip file."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    response = get_edinet_document(doc_id=doc_id)

    # If the request was successful, save the content to a file
    if response.status_code == 200:
        with open(os.path.join(folder_path, f"{doc_id}.zip"), "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
    # If the request was not successful, raise an exception
    elif response.status_code != 200:
        raise Exception(f"Error fetching data from EDINET API: {response.status_code}")


def download_edinet_documents(doc_list, folder_path) -> None:
    """Download multiple EDINET XBRL files as Zip files."""
    # Create the folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Iterate over the document list and download each documents
    for _, doc in doc_list.iterrows():
        download_edinet_document(doc_id=doc["docID"], folder_path=folder_path)
        unzip_file(file_name=doc["docID"], folder_path=folder_path, remove_zip=True)
        time.sleep(1)


def unzip_file(file_name, folder_path, remove_zip=False) -> None:
    """Unzip EDINET XBRL Zip file."""
    # Create the folder if it doesn't exist
    if not os.path.exists(os.path.join(folder_path, file_name)):
        os.makedirs(os.path.join(folder_path, file_name))

    # Unzip the file
    with zipfile.ZipFile(os.path.join(folder_path, f"{file_name}.zip"), "r") as zip_ref:
        zip_ref.extractall(os.path.join(folder_path, file_name))

    # Remove the zip file if specified
    if remove_zip:
        os.remove(os.path.join(folder_path, f"{file_name}.zip"))
