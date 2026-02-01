"""Test for api.

Need to do:
"""

import datetime
import io
import os
import tempfile
import zipfile
from functools import wraps
from unittest.mock import patch

import pandas as pd
import pytest
import yfinance as yf

from dfolks.data.edinet_apis import (
    download_edinet_document,
    download_edinet_documents,
    get_edinet_document,
    get_edinet_document_list,
    unzip_file,
)
from dfolks.data.jquants_apis import (
    get_jquants_corporate_list_v2,
    get_jquants_fin_report_v2,
    get_jquants_industry_report_v2,
    get_jquants_stock_price_v2,
)
from dfolks.data.yfinance_apis import (
    get_yfinance_balance_sheet,
    get_yfinance_cash_flow,
    get_yfinance_dividends,
    get_yfinance_income_statement,
    get_yfinance_info,
    get_yfinance_stock_prices,
    get_yfinance_ticker,
)

"""J-Quants API tests."""
# Test: get_jquants_api_refresh_token


# Test: get_jquants_corporate_list
@patch("dfolks.data.jquants_apis.requests.get")
def test_get_jquants_corporate_list_v2(mock_get):
    mock_get.return_value.json.return_value = {
        "data": [
            {"Code": "1234", "name": "Test Corp"},
            {"Code": "5678", "name": "Another Corp"},
        ]
    }
    api_key = "dummy_id_token"
    corporate_list = get_jquants_corporate_list_v2(api_key=api_key)
    assert isinstance(corporate_list, pd.DataFrame)
    assert corporate_list.shape == (2, 2)
    assert corporate_list["Code"].to_list() == ["1234", "5678"]


# Test: get_jquants_fin_report
@patch("dfolks.data.jquants_apis.requests.get")
def test_get_jquants_fin_report(mock_get):
    mock_get.return_value.json.return_value = {
        "data": [
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
    api_key = "dummy_id_token"
    code = "1234"
    date = "2023-01-01"

    df = get_jquants_fin_report_v2(api_key=api_key, code=code, date=date)
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
        "data": [
            {"Code": "1234", "Date": "2023-01-01", "Open": 100, "Close": 110},
            {"Code": "1234", "Date": "2023-01-02", "Open": 105, "Close": 115},
        ]
    }

    api_key = "dummy_id_token"
    code = "1234"

    df = get_jquants_stock_price_v2(api_key=api_key, code=code)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] == 2
    assert df["Code"].iloc[0] == "1234"
    assert df["Date"].iloc[0] == "2023-01-01"
    assert df["Open"].iloc[0] == 100
    assert df["Close"].iloc[0] == 110


# Test: get_jquants_industry_report
@patch("dfolks.data.jquants_apis.requests.get")
def test_get_jquants_industry_report(mock_get):
    mock_get.return_value.json.return_value = {
        "data": [
            {
                "Section": "AAAA",
                "PubDate": "2023-01-01",
                "PropSell": 100,
                "PropBuy": 110,
            },
            {
                "Section": "BBBB",
                "PubDate": "2023-01-02",
                "PropSell": 105,
                "PropBuy": 115,
            },
        ]
    }

    api_key = "dummy_id_token"

    df = get_jquants_industry_report_v2(api_key=api_key)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] == 2
    assert df["Section"].iloc[0] == "AAAA"
    assert df["PubDate"].iloc[0] == "2023-01-01"
    assert df["PropSell"].iloc[0] == 100
    assert df["PropBuy"].iloc[0] == 110


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


"""EDINET API tests."""
# Test: get_edinet_document_list


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


# Test: get_edinet_document
@patch("dfolks.data.edinet_apis.requests.get")
def test_get_edinet_document(mock_get, fake_zip_bytes):
    mock_get.return_value.status_code = 200
    mock_get.return_value.iter_content = lambda chunk_size: [fake_zip_bytes]

    doc_id = "ABC1234"

    # Patch environment variable for the test
    with patch.dict("os.environ", {"EDINET_API_TOKEN": "fake_token"}):
        response = get_edinet_document(doc_id)
    assert response.status_code == 200


# Test: download_edinet_document
@patch("dfolks.data.edinet_apis.requests.get")
def test_download_edinet_document(mock_get, fake_zip_bytes, temp_dir):
    mock_get.return_value.status_code = 200
    mock_get.return_value.iter_content = lambda chunk_size: [fake_zip_bytes]

    doc_id = "ABC1234"

    # Patch environment variable for the test
    with patch.dict("os.environ", {"EDINET_API_TOKEN": "fake_token"}):
        download_edinet_document(doc_id, temp_dir)

    zip_path = os.path.join(temp_dir, f"{doc_id}.zip")
    assert os.path.exists(zip_path)
    with open(zip_path, "rb") as f:
        content = f.read()
    assert content == fake_zip_bytes


# Test: download_edinet_document failure
@patch("dfolks.data.edinet_apis.requests.get")
def test_download_edinet_document_failure(mock_get, temp_dir):
    mock_get.return_value.status_code = 404

    doc_id = "NONEXISTENT"

    with pytest.raises(Exception) as excinfo:
        with patch.dict("os.environ", {"EDINET_API_TOKEN": "fake_token"}):
            download_edinet_document(doc_id, temp_dir)
    assert "Error fetching data from EDINET API: 404" in str(excinfo.value)


# Test: unzip_file
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


# Test: download_edinet_documents
@patch("dfolks.data.edinet_apis.download_edinet_document")
@patch("dfolks.data.edinet_apis.unzip_file")
def test_download_edinet_documents(mock_unzip, mock_download, temp_dir):
    """Test downloading multiple EDINET documents."""
    doc_list = pd.DataFrame({"docID": ["DOC1", "DOC2"]})

    with patch.dict("os.environ", {"EDINET_API_TOKEN": "fake_token"}):
        download_edinet_documents(doc_list, temp_dir)

    assert mock_download.call_count == 2
    assert mock_unzip.call_count == 2


"""yahoo finance API tests."""
# Mock up yf.ticker


class MockTicker:
    def __init__(self, ticker):
        self.ticker = ticker

        self.info = {
            "symbol": ticker,
            "shortName": "Test Company",
            "sector": "Technology",
        }

        self.income_stmt = pd.DataFrame(
            {
                "2023-12-31": {"Total Revenue": 1000, "Net Income": 100},
                "2022-12-31": {"Total Revenue": 900, "Net Income": 90},
            }
        )

        self.balance_sheet = pd.DataFrame(
            {
                "2023-12-31": {"Total Assets": 5000, "Total Liab": 2000},
                "2022-12-31": {"Total Assets": 4500, "Total Liab": 1800},
            }
        )

        self.cashflow = pd.DataFrame(
            {
                "2023-12-31": {"Operating Cash Flow": 300, "Investing Cash Flow": -100},
                "2022-12-31": {"Operating Cash Flow": 250, "Investing Cash Flow": -80},
            }
        )

        self.dividends = pd.DataFrame(
            {"Dividends": {"2023-06-01": 1.5, "2022-06-01": 1.4, "2021-06-01": 1.3}}
        )
        self.dividends.index.name = "Date"


# Decorator to mock yf.Ticker
def mock_yfinance_ticker(func):
    """Decorator to mock yf.Ticker"""

    @wraps(func)
    def wrapper(monkeypatch, *args, **kwargs):
        monkeypatch.setattr(yf, "Ticker", lambda t: MockTicker(t))
        return func(monkeypatch, *args, **kwargs)

    return wrapper


# Tests related to yf.Ticker
@mock_yfinance_ticker
def test_get_yfinance_ticker(monkeypatch):
    ticker = "TEST"
    yf_ticker = get_yfinance_ticker(ticker)
    assert isinstance(yf_ticker, MockTicker)
    assert yf_ticker.ticker == ticker


@mock_yfinance_ticker
def test_get_yfinance_info(monkeypatch):
    ticker = "TEST"
    df = get_yfinance_info(ticker)
    assert isinstance(df, pd.DataFrame)
    assert df["symbol"].iloc[0] == ticker
    assert df["shortName"].iloc[0] == "Test Company"
    assert df["sector"].iloc[0] == "Technology"


@mock_yfinance_ticker
def test_get_yfinance_income_statement(monkeypatch):
    ticker = "TEST"
    df = get_yfinance_income_statement(ticker)
    cols = ["date", "Total Revenue", "Net Income", "ticker"]
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2  # Two periods
    assert set(cols).issubset(df.columns)
    assert df["ticker"].iloc[0] == ticker


@mock_yfinance_ticker
def test_get_yfinance_balance_sheet(monkeypatch):
    ticker = "TEST"
    df = get_yfinance_balance_sheet(ticker)
    cols = ["date", "Total Assets", "Total Liab", "ticker"]
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2  # Two periods
    assert set(cols).issubset(df.columns)
    assert df["ticker"].iloc[0] == ticker


@mock_yfinance_ticker
def test_get_yfinance_cash_flow(monkeypatch):
    ticker = "TEST"
    df = get_yfinance_cash_flow(ticker)
    cols = ["date", "Operating Cash Flow", "Investing Cash Flow", "ticker"]
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2  # Two periods
    assert set(cols).issubset(df.columns)
    assert df["ticker"].iloc[0] == ticker


@mock_yfinance_ticker
def test_get_yfinance_dividends(monkeypatch):
    ticker = "TEST"
    df = get_yfinance_dividends(ticker)
    cols = ["date", "Dividends", "ticker"]
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 3  # Three dividend entries
    assert set(cols).issubset(df.columns)
    assert df["ticker"].iloc[0] == ticker


# Mock up yf.download
def mock_yfinance_download(func):
    @wraps(func)
    def wrapper(monkeypatch, *args, **kwargs):
        def fake_download(*args, **kwargs):
            cols = pd.MultiIndex.from_product(
                [["Test"], ["Open", "High", "Low"]],
                names=["Ticker", "Price"],
            )
            df = pd.DataFrame(
                [[1, 2, 3]],
                index=[pd.Timestamp("2025-01-01")],
                columns=cols,
            )
            df.index.name = "Date"
            return df

        monkeypatch.setattr(yf, "download", fake_download)
        return func(monkeypatch, *args, **kwargs)

    return wrapper


@mock_yfinance_download
def test_get_yfinance_stock_prices(monkeypatch):
    ticker = "Test"
    start_date = "2025-01-01"
    end_date = "2025-01-31"

    df = get_yfinance_stock_prices(ticker, start_date=start_date, end_date=end_date)
    expected_cols = ["date", "Open", "High", "Low", "ticker"]

    assert isinstance(df, pd.DataFrame)
    assert set(expected_cols).issubset(df.columns)
    assert df.shape[0] == 1  # One date entry
    assert df["ticker"].iloc[0] == ticker
