"""Google APIs.

Need to do
0) Documentation
"""

import io
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]

# Define the path to the .env file
env_path = Path(__file__).resolve().parents[3] / "env" / ".env"

# Load environment variables from .env file
load_dotenv(dotenv_path=env_path)

# Path setup (from .env)
CREDENTIALS_PATH = os.getenv("GOOGLE_API_CREDENTIALS_PATH")
TOKEN_PATH = os.getenv("GOOGLE_API_TOKEN_PATH")

# Set up a shared logger
logger = logging.getLogger("shared")


def google_drive_authenticate():
    """Authenticate with Google Drive API and return a service client."""
    creds = None

    # Load saved token if available
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    # Refresh or obtain new credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save credentials for future runs
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    service = build("drive", "v3", credentials=creds)
    return service


def google_drive_upload_file(
    file_path, mime_type="application/octet-stream", folder_id=None
):
    """Upload a file to Google Drive."""
    service = google_drive_authenticate()
    file_metadata = {"name": os.path.basename(file_path)}
    if folder_id:
        file_metadata["parents"] = [folder_id]

    media = MediaFileUpload(file_path, mimetype=mime_type)
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    logger.info(f"Uploaded '{file_path}' → File ID: {file.get('id')}")
    return file.get("id")


def google_drive_download_file(file_id, destination_path):
    """Download a file from Google Drive by file ID."""
    service = google_drive_authenticate()
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(f"Download {int(status.progress() * 100)}%.")
    logger.info(f"Download complete: {destination_path}")
    return destination_path


def google_drive_list_files(limit=10):
    """List files in your Google Drive (for testing)."""
    service = google_drive_authenticate()
    results = service.files().list(pageSize=limit, fields="files(id, name)").execute()
    files = results.get("files", [])
    for f in files:
        print(f"{f['name']} ({f['id']})")
    return files
