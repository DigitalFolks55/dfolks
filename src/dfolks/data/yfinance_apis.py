"""API calls to get data from Yahoo finance.

Need to do
0) Add more api calls.
"""

import pandas as pd
import yfinance as yf


def get_yfinance_ticker(ticker: str) -> yf.Ticker:
    """Get yfinance Ticker object."""
    return yf.Ticker(ticker)


def get_yfinance_info(ticker: str) -> pd.DataFrame:
    """Get yfinance Ticker info."""
    yf_ticker = get_yfinance_ticker(ticker)
    df = pd.DataFrame(yf_ticker.info.items(), columns=["key", "value"])
    df_pivot = df.set_index("key").transpose().reset_index(drop=True)

    return df_pivot


def get_yfinance_income_statement(ticker: str) -> pd.DataFrame:
    """Get yfinance Ticker income statement."""
    yf_ticker = get_yfinance_ticker(ticker)
    df = yf_ticker.income_stmt.transpose().reset_index()
    df["ticker"] = ticker
    df.rename(columns={"index": "date"}, inplace=True)

    return df


def get_yfinance_balance_sheet(ticker: str) -> pd.DataFrame:
    """Get yfinance Ticker balance sheet."""
    yf_ticker = get_yfinance_ticker(ticker)
    df = yf_ticker.balance_sheet.transpose().reset_index()
    df["ticker"] = ticker
    df.rename(columns={"index": "date"}, inplace=True)

    return df


def get_yfinance_cash_flow(ticker: str) -> pd.DataFrame:
    """Get yfinance Ticker cash flow statement."""
    yf_ticker = get_yfinance_ticker(ticker)
    df = yf_ticker.cashflow.transpose().reset_index()
    df["ticker"] = ticker
    df.rename(columns={"index": "date"}, inplace=True)

    return df


def get_yfinance_dividends(ticker: str) -> pd.DataFrame:
    """Get yfinance Ticker dividends."""
    yf_ticker = get_yfinance_ticker(ticker)
    df = pd.DataFrame(yf_ticker.dividends).reset_index()
    df["ticker"] = ticker
    df.rename(columns={"Date": "date"}, inplace=True)

    return df


def get_yfinance_stock_prices(
    tickers: list,
    period: str = None,
    interval: str = None,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """Get yfinance Ticker stock prices."""
    if period is not None and (start_date is not None and end_date is not None):
        raise ValueError(
            "If period provided then start_date or end_date is not required."
        )
    elif period is None and (start_date is None or end_date is None):
        raise ValueError(
            "If period is not provided then start_date and end_date are required."
        )

    if period is not None:
        if interval is not None:
            yf_stock = yf.download(
                tickers,
                period=period,
                interval=interval,
                group_by="ticker",
                threads=True,
            )
        else:
            yf_stock = yf.download(
                tickers, period=period, group_by="ticker", threads=True
            )
    elif period is None and (start_date is not None and end_date is not None):
        yf_stock = yf.download(
            tickers, start=start_date, end=end_date, group_by="ticker", threads=True
        )

    df = yf_stock.stack(level=0).reset_index()
    df.rename(columns={"Date": "date", "Ticker": "ticker"}, inplace=True)

    return df
