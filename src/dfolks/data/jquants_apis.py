"""API calls to get data from JQUANTS.

Need to do
0) Add more api calls.
1) Add loggers with HTTP status codes.
"""

import datetime
import json
import os
import pickle
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
    # Get E-mail address & password from .env file
    data = {
        "mailaddress": os.getenv("JQUANTS_API_EMAIL_ADDRESS"),
        "password": os.getenv("JQUANTS_API_PASSWORD"),
    }
    # Refresh token request
    r_post = requests.post(
        "https://api.jquants.com/v1/token/auth_user", data=json.dumps(data)
    )
    # Store refresh token
    refreshToken = r_post.json().get("refreshToken")

    if not refreshToken:
        raise ValueError("Failed to retrieve refresh token. Check your credentials.")

    return refreshToken


def get_jquants_api_token(refreshToken: str = None) -> str:
    """Get J-Quants API token."""
    # If refreshToken is None, get new one
    if refreshToken is None:
        refreshToken = get_jquants_api_refresh_token()

    if not refreshToken:
        raise ValueError("Refresh token is required to get J-Quants API token.")

    jquants_refresh_token = refreshToken
    # Get idToken
    r = requests.post(
        f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={jquants_refresh_token}"
    )
    # Store idToken
    idToken = r.json().get("idToken")

    if not idToken:
        raise ValueError(
            "Failed to retrieve J-Quants API token. Check your refresh token."
        )

    return idToken


def update_jquants_tokens(save_as_file=True) -> str:
    """Update J-Quants API token if it is expired."""
    jquants_token_path = os.getenv("JQUANTS_TOKEN_PATH")
    now = datetime.datetime.now()

    # If a token file (pickle) exists then extract the token from the file
    if os.path.exists(jquants_token_path):
        with open(jquants_token_path, "rb") as f:
            tokens = pickle.load(f)
            refreshToken_datetime = tokens.get("refreshToken").get("datetime")
            refreshToken = tokens.get("refreshToken").get("token")
            idToken_datetime = tokens.get("idToken").get("datetime")
            idToken = tokens.get("idToken").get("token")

        # If the refresh token is expired (> 7days, but set up 6days), get new refresh tokens
        if (now - refreshToken_datetime).days >= 6:
            refreshToken = get_jquants_api_refresh_token()
        # If the id token is expired (> 1day but set up 23hours), get a new one
        if (now - idToken_datetime).total_seconds() >= 60 * 60 * 23:
            idToken = get_jquants_api_token(refreshToken)
    else:
        refreshToken = get_jquants_api_refresh_token()
        idToken = get_jquants_api_token(refreshToken)

    # Save tokens as a pickle file, if variable save_as_file is True
    if save_as_file:
        tokens = {
            "refreshToken": {
                "datetime": now,
                "token": refreshToken,
            },
            "idToken": {
                "datetime": now,
                "token": idToken,
            },
        }

        with open(jquants_token_path, "wb") as f:
            pickle.dump(tokens, f)

    return refreshToken, idToken


def get_jquants_corporate_list(idToken: str) -> list:
    """Get J-Quants corporate list.

    Get data at the point of exectuion; date not defined.
    """

    headers = {"Authorization": "Bearer {}".format(idToken)}
    r = requests.get("https://api.jquants.com/v1/listed/info", headers=headers)

    df = pd.DataFrame(r.json().get("info"))

    return df


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
        # With single date
        r = requests.get(
            f"https://api.jquants.com/v1/prices/daily_quotes?code={code}&date={date}",
            headers=headers,
        )
    elif date_from and date_to:
        # With date range
        r = requests.get(
            f"https://api.jquants.com/v1/prices/daily_quotes?code={code}&from={date_from}&to={date_to}",
            headers=headers,
        )
    else:
        # Without dates
        r = requests.get(
            f"https://api.jquants.com/v1/prices/daily_quotes?code={code}",
            headers=headers,
        )

    df = pd.DataFrame(r.json()["daily_quotes"])

    return df
