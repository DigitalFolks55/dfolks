"""Test for api.

Need to do:
"""

import datetime
import io
import os
import pickle
import tempfile
import zipfile
from unittest.mock import patch

import pandas as pd
import pytest

from dfolks.data.edinet_apis import (
    download_edinet_document,
    download_edinet_documents,
    get_edinet_document,
    get_edinet_document_list,
    unzip_file,
)
from dfolks.data.jquants_apis import (
    get_jquants_api_refresh_token,
    get_jquants_api_token,
    get_jquants_corporate_list,
    get_jquants_fin_report,
    get_jquants_stock_price,
    update_jquants_tokens,
)


# Test: get_jquants_api_refresh_token
@patch("dfolks.data.jquants_apis.requests.post")
@patch("dfolks.data.jquants_apis.os.getenv")
def test_get_jquants_api_refresh_token(mock_getenv, mock_post):
    mock_getenv.side_effect = lambda key: {
        "JQUANTS_API_EMAIL_ADDRESS": "test@test.com",
        "JQUANTS_API_PASSWORD": "dummy_password",
    }.get(key)

    mock_post.return_value.json.return_value = {"refreshToken": "dummy_refreshtoken"}

    refreshToken = get_jquants_api_refresh_token()
    assert refreshToken == "dummy_refreshtoken"


# Test: get_jquants_api_token
@patch("dfolks.data.jquants_apis.requests.post")
def test_get_jquants_api_token(mock_post):
    mock_post.return_value.json.return_value = {"idToken": "dummy_id_token"}
    idToken = get_jquants_api_token(refreshToken="dummy_refreshtoken")
    assert idToken == "dummy_id_token"


# Test: get_jquants_corporate_list
@patch("dfolks.data.jquants_apis.requests.get")
def test_get_jquants_corporate_list(mock_get):
    mock_get.return_value.json.return_value = {
        "info": [
            {"Code": "1234", "name": "Test Corp"},
            {"Code": "5678", "name": "Another Corp"},
        ]
    }
    idToken = "dummy_id_token"
    corporate_list = get_jquants_corporate_list(idToken=idToken)
    assert isinstance(corporate_list, list)
    assert len(corporate_list) == 2
    assert corporate_list == ["1234", "5678"]


# Test: get_jquants_fin_report
@patch("dfolks.data.jquants_apis.requests.get")
def test_get_jquants_fin_report(mock_get):
    mock_get.return_value.json.return_value = {
        "statements": [
            {
                "LocalCode": "1234",
                "DisclosedDate": "2023-01-01",
                "TypeOfDocument": "Annual",
            },
            {
                "LocalCode": "1234",
                "DisclosedDate": "2023-01-02",
                "TypeOfDocument": "Quarter",
            },
        ]
    }
    idToken = "dummy_id_token"
    code = "1234"
    date = "2023-01-01"

    df = get_jquants_fin_report(idToken=idToken, code=code, date=date)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] == 2
    assert df["LocalCode"].iloc[0] == "1234"
    assert df["DisclosedDate"].iloc[0] == "2023-01-01"
    assert df["TypeOfDocument"].iloc[0] == "Annual"


# Test: get_jquants_stock_price
@patch("dfolks.data.jquants_apis.requests.get")
def test_get_jquants_stock_price(mock_get):
    mock_get.return_value.json.return_value = {
        "daily_quotes": [
            {"Code": "1234", "Date": "2023-01-01", "Open": 100, "Close": 110},
            {"Code": "1234", "Date": "2023-01-02", "Open": 105, "Close": 115},
        ]
    }
    idToken = "dummy_id_token"
    code = "1234"

    df = get_jquants_stock_price(idToken=idToken, code=code)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] == 2
    assert df["Code"].iloc[0] == "1234"
    assert df["Date"].iloc[0] == "2023-01-01"
    assert df["Open"].iloc[0] == 100
    assert df["Close"].iloc[0] == 110


@pytest.fixture
def expired_tokens():
    now = datetime.datetime.now()
    return {
        "refreshToken": {
            "datetime": now - datetime.timedelta(days=7),
            "token": "expired_refresh",
        },
        "idToken": {
            "datetime": now - datetime.timedelta(hours=24),
            "token": "expired_id",
        },
    }


@pytest.fixture
def temp_dir():
    """Create a temporary folder for tests (auto-cleaned)."""
    with tempfile.TemporaryDirectory() as tempdir:
        yield tempdir


@patch("dfolks.data.jquants_apis.get_jquants_api_refresh_token")
@patch("dfolks.data.jquants_apis.get_jquants_api_token")
def test_update_with_existing_expired_tokens_jquants(
    mock_get_id, mock_get_refresh, expired_tokens, monkeypatch, temp_dir
):
    mock_get_refresh.return_value = "mock_refresh_token"
    mock_get_id.return_value = "mock_id_token"

    # Write expired tokens to simulate existing file
    token_path = os.path.join(temp_dir, "new_token.pkl")
    monkeypatch.setenv("JQUANTS_TOKEN_PATH", token_path)

    with open(token_path, "wb") as f:
        pickle.dump(expired_tokens, f)

    refresh, id_ = update_jquants_tokens(save_as_file=True)

    assert refresh == "mock_refresh_token"
    assert id_ == "mock_id_token"

    # Check updated tokens were saved
    with open(token_path, "rb") as f:
        saved = pickle.load(f)
    assert saved["refreshToken"]["token"] == "mock_refresh_token"
    assert saved["idToken"]["token"] == "mock_id_token"


@patch("dfolks.data.jquants_apis.get_jquants_api_refresh_token")
@patch("dfolks.data.jquants_apis.get_jquants_api_token")
def test_update_without_existing_file_jquants(
    mock_get_id, mock_get_refresh, monkeypatch, temp_dir
):
    mock_get_refresh.return_value = "new_refresh_token"
    mock_get_id.return_value = "new_id_token"

    token_path = os.path.join(temp_dir, "new_token.pkl")
    monkeypatch.setenv("JQUANTS_TOKEN_PATH", token_path)

    refresh, id_ = update_jquants_tokens(save_as_file=True)

    assert refresh == "new_refresh_token"
    assert id_ == "new_id_token"
    assert os.path.exists(token_path)

    with open(token_path, "rb") as f:
        tokens = pickle.load(f)
    assert tokens["refreshToken"]["token"] == "new_refresh_token"
    assert tokens["idToken"]["token"] == "new_id_token"


@patch("dfolks.data.edinet_apis.requests.get")
def test_get_edinet_document_list(mock_get):
    mock_get.return_value.json.return_value = {
        "results": [
            {
                "docID": "ABC1234",
                "edinetCode": "EDI100",
                "secCode": "2000",
                "filerName": "TESTCOM",
                "ordinanceCode": "010",
                "formCode": "03000",
                "docType": "120",
                "submitDateTime": "2025-01-01",
            },
        ]
    }
    mock_get.return_value.status_code = 200

    date = "2025-01-01"
    # Patch environment variable for the test
    with patch.dict("os.environ", {"EDINET_API_TOKEN": "fake_token"}):
        df = get_edinet_document_list(date)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] == 1
    assert df["submitDateTime"].iloc[0] == date
    assert df["edinetCode"].iloc[0] == "EDI100"
    assert df["secCode"].iloc[0] == "2000"
    assert df["docID"].iloc[0] == "ABC1234"


@pytest.fixture
def fake_zip_bytes():
    memory_zip = io.BytesIO()
    with zipfile.ZipFile(memory_zip, mode="w") as zf:
        zf.writestr("testfile.txt", "This is a test file.")
    memory_zip.seek(0)
    return memory_zip.getvalue()


@patch("dfolks.data.edinet_apis.requests.get")
def test_get_edinet_document(mock_get, fake_zip_bytes):
    mock_get.return_value.status_code = 200
    mock_get.return_value.iter_content = lambda chunk_size: [fake_zip_bytes]

    doc_id = "ABC1234"
    response = get_edinet_document(doc_id)
    assert response.status_code == 200


@patch("dfolks.data.edinet_apis.requests.get")
def test_download_edinet_document(mock_get, fake_zip_bytes, temp_dir):
    mock_get.return_value.status_code = 200
    mock_get.return_value.iter_content = lambda chunk_size: [fake_zip_bytes]

    doc_id = "ABC1234"

    download_edinet_document(doc_id, temp_dir)

    zip_path = os.path.join(temp_dir, f"{doc_id}.zip")
    assert os.path.exists(zip_path)
    with open(zip_path, "rb") as f:
        content = f.read()
    assert content == fake_zip_bytes


@patch("dfolks.data.edinet_apis.requests.get")
def test_download_edinet_document_failure(mock_get, temp_dir):
    mock_get.return_value.status_code = 404

    doc_id = "NONEXISTENT"

    with pytest.raises(Exception) as excinfo:
        download_edinet_document(doc_id, temp_dir)
    assert "Error fetching data from EDINET API: 404" in str(excinfo.value)


def test_unzip_file_extracts_and_removes_zip(temp_dir):
    """Test that unzip_file extracts and optionally removes the zip."""
    doc_id = "S100ABC"
    zip_path = os.path.join(temp_dir, f"{doc_id}.zip")

    # Create a fake zip file
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.txt", "sample data")

    unzip_file(file_name=doc_id, folder_path=temp_dir, remove_zip=True)

    extracted_file = os.path.join(temp_dir, doc_id, "data.txt")
    assert os.path.exists(extracted_file)
    assert not os.path.exists(zip_path)  # zip should be removed

    with open(extracted_file, "r") as f:
        assert f.read() == "sample data"


@patch("dfolks.data.edinet_apis.download_edinet_document")
@patch("dfolks.data.edinet_apis.unzip_file")
def test_download_edinet_documents(mock_unzip, mock_download, temp_dir):
    """Test downloading multiple EDINET documents."""
    doc_list = pd.DataFrame({"docID": ["DOC1", "DOC2"]})

    download_edinet_documents(doc_list, temp_dir)

    assert mock_download.call_count == 2
    assert mock_unzip.call_count == 2
