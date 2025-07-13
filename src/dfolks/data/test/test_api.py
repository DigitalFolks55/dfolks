"""Test for api.

Need to do:
"""

from unittest.mock import patch

import pandas as pd

from dfolks.data.jquants_apis import (
    get_jquants_api_refresh_token,
    get_jquants_api_token,
    get_jquants_corporate_list,
    get_jquants_fin_report,
    get_jquants_stock_price,
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
