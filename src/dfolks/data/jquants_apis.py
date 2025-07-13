"""API calls to get data from JQUANTS.

Need to do
0) Enable pydantic to validate variables.
1) Add more apis.
"""

import json
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Define the path to the .env file
env_path = Path(__file__).resolve().parents[3] / "env" / ".env"

# Load environment variables from .env file
load_dotenv(dotenv_path=env_path)


def get_jquants_api_refresh_token() -> str:
    """Get J-Quants API refesh token."""
    os.getenv("JQUANTS_API_REFRESH_TOKEN")
    data = {
        "mailaddress": os.getenv("JQUANTS_API_EMAIL_ADDRESS"),
        "password": os.getenv("JQUANTS_API_PASSWORD"),
    }
    r_post = requests.post(
        "https://api.jquants.com/v1/token/auth_user", data=json.dumps(data)
    )
    refreshToken = r_post.json().get("refreshToken")

    if not refreshToken:
        raise ValueError("Failed to retrieve refresh token. Check your credentials.")

    return refreshToken


def get_jquants_api_token(refreshToken: str = None) -> str:
    """Get J-Quants API token."""
    if refreshToken is None:
        refreshToken = get_jquants_api_refresh_token()

    if not refreshToken:
        raise ValueError("Refresh token is required to get J-Quants API token.")

    jquants_refresh_token = refreshToken
    r = requests.post(
        f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={jquants_refresh_token}"
    )
    idToken = r.json().get("idToken")

    if not idToken:
        raise ValueError(
            "Failed to retrieve J-Quants API token. Check your refresh token."
        )

    return idToken


def get_jquants_corporate_list(idToken: str) -> list:
    """Get J-Quants corporate list.

    Get data at the point of exectuion; date not defined.
    """
    corp_list = []

    headers = {"Authorization": "Bearer {}".format(idToken)}
    r = requests.get("https://api.jquants.com/v1/listed/info", headers=headers)

    for dic in r.json().get("info"):
        corp_list.append(dic.get("Code"))

    return corp_list


def get_jquants_fin_report(idToken: str, code: int, date: str = None) -> pd.DataFrame:
    """Get J-Quants financial statements."""

    headers = {"Authorization": "Bearer {}".format(idToken)}
    if date:
        r = requests.get(
            f"https://api.jquants.com/v1/fins/statements?code={code}&date={date}",
            headers=headers,
        )
    else:
        r = requests.get(
            f"https://api.jquants.com/v1/fins/statements?code={code}", headers=headers
        )

    df = pd.DataFrame(r.json()["statements"])

    return df


def get_jquants_stock_price(
    idToken: str,
    code: int,
    date: str = None,
    date_from: str = None,
    date_to: str = None,
) -> pd.DataFrame:
    """Get J-Quants financial statements."""

    if date and (date_from or date_to):
        raise ValueError(
            "Not accepted: 'date' and 'date_from' or 'date_to' at the same time."
        )

    headers = {"Authorization": "Bearer {}".format(idToken)}
    if date:
        r = requests.get(
            f"https://api.jquants.com/v1/prices/daily_quotes?code={code}&date={date}",
            headers=headers,
        )
    elif date_from and date_to:
        r = requests.get(
            f"https://api.jquants.com/v1/prices/daily_quotes?code={code}&from={date_from}&to={date_to}",
            headers=headers,
        )
    else:
        r = requests.get(
            f"https://api.jquants.com/v1/prices/daily_quotes?code={code}",
            headers=headers,
        )

    df = pd.DataFrame(r.json()["daily_quotes"])

    return df
