import os
import unittest
from unittest.mock import MagicMock, patch

from dfolks.utils.google_api import (
    google_drive_authenticate,
    google_drive_download_file,
    google_drive_list_files,
    google_drive_upload_file,
)


class TestGoogleDriveAPI(unittest.TestCase):

    def setUp(self):
        """Set fake environment variables before each test."""
        os.environ["GOOGLE_API_CREDENTIALS_PATH"] = "/fake/credentials.json"
        os.environ["GOOGLE_API_TOKEN_PATH"] = "/fake/token.json"

    @patch("builtins.open", new_callable=MagicMock)  # Mock open to prevent file access
    @patch(
        "os.path.exists", return_value=True
    )  # Mock os.path.exists to always return True
    @patch("dfolks.utils.google_api.build")
    @patch("dfolks.utils.google_api.Credentials")
    @patch("dfolks.utils.google_api.InstalledAppFlow")
    def test_google_drive_authenticate(
        self, mock_flow, mock_credentials, mock_build, mock_exists, mock_open
    ):
        """Test authentication builds and returns a Drive service."""
        # Mock credentials
        mock_creds_instance = MagicMock(valid=True)
        mock_credentials.from_authorized_user_file.return_value = mock_creds_instance

        # Mock InstalledAppFlow to prevent file access
        mock_flow_instance = MagicMock()
        mock_flow.from_client_secrets_file.return_value = mock_flow_instance
        mock_flow_instance.run_local_server.return_value = mock_creds_instance

        # Mock the Drive service build
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        # Call function
        service = google_drive_authenticate()

        # Verify build() was called correctly
        mock_build.assert_called_once_with(
            "drive", "v3", credentials=mock_creds_instance
        )
        self.assertEqual(service, mock_service)

    @patch("dfolks.utils.google_api.MediaFileUpload")
    @patch("dfolks.utils.google_api.google_drive_authenticate")
    def test_upload_file(self, mock_auth, mock_media_upload):
        """Test upload_file creates file and returns file ID."""
        mock_service = MagicMock()
        mock_auth.return_value = mock_service

        # Mock Drive files().create().execute() chain
        mock_files = mock_service.files.return_value
        mock_create = mock_files.create.return_value
        mock_create.execute.return_value = {"id": "12345", "name": "test.txt"}

        # Mock the MediaFileUpload constructor (prevent FileNotFoundError)
        mock_media_upload.return_value = MagicMock()

        file_id = google_drive_upload_file("test.txt")

        self.assertEqual(file_id, "12345")

    @patch("dfolks.utils.google_api.io.FileIO")
    @patch("dfolks.utils.google_api.MediaIoBaseDownload")
    @patch("dfolks.utils.google_api.google_drive_authenticate")
    def test_download_file(self, mock_auth, mock_media_download, mock_fileio):
        """Test download_file downloads file correctly."""
        mock_service = MagicMock()
        mock_auth.return_value = mock_service

        # Mock Drive files().get_media()
        mock_files = mock_service.files.return_value
        mock_files.get_media.return_value = MagicMock()

        # Mock file handle (prevents writing to disk)
        mock_fh = MagicMock()
        mock_fileio.return_value = mock_fh

        # Mock downloader chunks
        mock_downloader = MagicMock()
        mock_downloader.next_chunk.side_effect = [
            (MagicMock(progress=lambda: 0.5), False),
            (MagicMock(progress=lambda: 1.0), True),
        ]
        mock_media_download.return_value = mock_downloader

        result = google_drive_download_file("12345", "output.txt")

        self.assertEqual(result, "output.txt")

    @patch("dfolks.utils.google_api.google_drive_authenticate")
    def test_list_files(self, mock_auth):
        """Test list_files retrieves file list correctly."""
        mock_service = MagicMock()
        mock_auth.return_value = mock_service

        # Mock Drive files().list().execute() chain
        mock_files = mock_service.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {"files": [{"id": "1", "name": "a.txt"}]}

        files = google_drive_list_files(limit=1)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["name"], "a.txt")


if __name__ == "__main__":
    unittest.main()
