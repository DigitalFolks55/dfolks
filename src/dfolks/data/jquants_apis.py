"""API calls to get data from JQUANTS.
Updated v1 to v2 on 2026-01-25.
Ref: https://jpx-jquants.com/ja/spec

Need to do
0) Add more api calls.
1) Add loggers with HTTP status codes.
"""

import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Define the path to the .env file
env_path = Path(__file__).resolve().parents[3] / "env" / ".env"

# Load environment variables from .env file
load_dotenv(dotenv_path=env_path)


def get_jquants_api_key_v2() -> str:
    """Get J-Quants API key from environment variables."""
    api_key = os.getenv("JQUANTS_API_KEY")
    if api_key is None:
        raise ValueError("JQUANTS_API_KEY not found in environment variables.")
    return api_key


def get_jquants_corporate_list_v2(api_key: str) -> list:
    """Get J-Quants corporate list.

    Get data at the point of exectuion; date not defined.
    """

    if api_key is None:
        raise ValueError("API key is required to use J-Quants API v2.")

    headers = {"x-api-key": api_key}
    r = requests.get("https://api.jquants.com/v2/equities/master", headers=headers)

    df = pd.DataFrame(r.json().get("data"))

    return df


def get_jquants_fin_report_v2(
    api_key: str, code: int = None, date: str = None
) -> pd.DataFrame:
    """Get J-Quants financial statements v2."""

    if api_key is None:
        raise ValueError("API key is required to use J-Quants API v2.")

    if code is None and date is None:
        raise ValueError("At least one of 'code' or 'date' must be provided.")

    headers = {"x-api-key": api_key}
    # API call based on given parameters
    if code is None and date is not None:
        r = requests.get(
            "https://api.jquants.com/v2/fins/summary",
            params={"date": date},
            headers=headers,
        )
    elif code is not None and date is None:
        r = requests.get(
            "https://api.jquants.com/v2/fins/summary",
            params={"code": code},
            headers=headers,
        )
    elif code is not None and date is not None:
        r = requests.get(
            "https://api.jquants.com/v2/fins/summary",
            params={"code": code, "date": date},
            headers=headers,
        )
    else:
        raise ValueError(
            "Process was not properly handled; check whether code and date are correct values and type."
        )

    df = pd.DataFrame(r.json()["data"])

    return df


def get_jquants_stock_price_v2(
    api_key: str,
    code: int = None,
    date: str = None,
    date_from: str = None,
    date_to: str = None,
) -> pd.DataFrame:
    """Get J-Quants financial statements."""

    if api_key is None:
        raise ValueError("API key is required to use J-Quants API v2.")

    if code is None and date is None:
        raise ValueError("At least one of 'code' or 'date' must be provided.")
    elif code is None and (date_from or date_to):
        raise ValueError(
            "If 'date_from' or 'date_to' is provided, 'code' must also be provided."
        )
    elif date and (date_from or date_to):
        raise ValueError(
            "Not accepted: 'date' and 'date_from' or 'date_to' at the same time."
        )

    headers = {"x-api-key": api_key}
    # API call based on given parameters
    if code is not None and date is not None:
        r = requests.get(
            "https://api.jquants.com/v2/equities/bars/daily",
            params={"code": code, "date": date},
            headers=headers,
        )
    elif code is not None and date_from and date_to:
        r = requests.get(
            "https://api.jquants.com/v2/equities/bars/daily",
            params={"code": code, "from": date_from, "to": date_to},
            headers=headers,
        )
    elif code is None and date is not None:
        r = requests.get(
            "https://api.jquants.com/v2/equities/bars/daily",
            params={"date": date},
            headers=headers,
        )
    else:
        r = requests.get(
            "https://api.jquants.com/v2/equities/bars/daily",
            params={"code": code},
            headers=headers,
        )

    df = pd.DataFrame(r.json()["data"])

    return df


def get_jquants_industry_report_v2(
    api_key: str, section: str = None, date_from: str = None, date_to: str = None
) -> pd.DataFrame:
    """Get J-Quants industry info v2."""

    if section is None and date_from is None and date_to is None:
        print(
            "None of section and date range were provided. All sectors and all dates are used."
        )

    headers = {"x-api-key": api_key}
    # API call based on given parameters
    if section is not None and date_from is not None and date_to is not None:
        r = requests.get(
            "https://api.jquants.com/v2/equities/investor-types",
            params={"section": section, "from": date_from, "to": date_to},
            headers=headers,
        )
    elif section is not None and date_from is None and date_to is None:
        r = requests.get(
            "https://api.jquants.com/v2/equities/investor-types",
            params={"section": section},
            headers=headers,
        )
    elif section is None and date_from is not None and date_to is not None:
        r = requests.get(
            "https://api.jquants.com/v2/equities/investor-types",
            params={"from": date_from, "to": date_to},
            headers=headers,
        )
    else:
        r = requests.get(
            "https://api.jquants.com/v2/equities/investor-types",
            headers=headers,
        )

    df = pd.DataFrame(r.json().get("data"))

    return df
