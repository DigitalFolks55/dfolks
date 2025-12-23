"""
Workflow for data ingestion via Yahoo Finance.

Need to work:
0) Documentation.
1) Add more error handling.
2) Add file type; parquet.
3) For other data.
"""

import datetime
import logging
import time
from typing import ClassVar, Dict, List, Optional

import pandas as pd
from pydantic import Field, field_validator

from dfolks.core.classfactory import WorkflowsRegistry
from dfolks.core.mixin import ExternalFileMixin
from dfolks.data.data import Validator
from dfolks.data.jquants_apis import (
    get_jquants_corporate_list,
    update_jquants_tokens,
)
from dfolks.data.output import SaveFile
from dfolks.data.yfinance_apis import (
    get_yfinance_balance_sheet,
    get_yfinance_cash_flow,
    get_yfinance_dividends,
    get_yfinance_income_statement,
    get_yfinance_stock_prices,
)
from dfolks.utils.utils import extract_primary_keys

# Set up shared logger
logger = logging.getLogger("shared")


class DataIngestionYFinanceFinReport(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for annual data ingestion via Yahoo Finance.

    Description: Financial Report Data Ingestion via Yahoo Finance API.

    Key methods
    ----------
    run: Abstract method.
        Execute overall workflow. To be implemented at subclasses.
    logger: set up a logger for workflow.
    variables: Return variables of the workflow.
    ----------

    Variables
    ----------
    corp_codes: Corporate codes to ingest data for.
        List[str] = None
    corp_filter_col: Column name for corporation filtering.
        str = None
    corp_filter: Filter string for corporation filtering (using regex).
        str = None
    cut_date: Date where we extract data, use this date in 'YYYY-MM-DD' format.
        str = None
    format: Output format.
        str = "df"
    target_db: Folder or Database path to save the ingested data.
        Optional[str] = None
    target_path_income_statement: Full file path to save the income statement data.
        Optional[str] = None
    target_path_balance_sheet: Full file path to save the balance sheet.
        Optional[str] = None
    target_path_cash_flow: Full file path to save the cash flow.
        Optional[str] = None
    target_path_dividends: Full file path to save the dividends.
        Optional[str] = None
    write_mode: File write mode.
        str = "overwrite"
    missing_col_impute: If True, impute missing columns based on schema; Due to data missing due to API.
        bool = True
    schema_income_statement: Output data schema of income statement.
        Optional[Dict] = Field(description="data_schema_income_statement.", default=None)
    schema_balance_sheet: Output data schema of income statement.
        Optional[Dict] = Field(description="data_schema_balance_sheet.", default=None)
    schema_cash_flow: Output data schema of income statement.
        Optional[Dict] = Field(description="data_schema_cash_flow.", default=None)
    schema_dividends: Output data schema of income statement.
        Optional[Dict] = Field(description="data_schema_dividends.", default=None)
    ----------
    """

    # variables
    wfclss: ClassVar[str] = "DataIngestionYFinanceFinReport"

    kind: str = "DataIngestionYFinanceFinReport"
    corp_codes: List[str] = None
    corp_filter_col: str = None
    corp_filter: str = None
    cut_date: str = None
    format: str = "df"
    target_db: Optional[str] = None
    target_path_income_statement: Optional[str] = None
    target_path_balance_sheet: Optional[str] = None
    target_path_cash_flow: Optional[str] = None
    target_path_dividends: Optional[str] = None
    write_mode: str = "overwrite"
    missing_col_impute: bool = True
    schema_income_statement: Optional[Dict] = Field(
        description="data_schema_income_statement.", default=None
    )
    schema_balance_sheet: Optional[Dict] = Field(
        description="data_schema_balance_sheet.", default=None
    )
    schema_cash_flow: Optional[Dict] = Field(
        description="data_schema_cash_flow.", default=None
    )
    schema_dividends: Optional[Dict] = Field(
        description="data_schema_dividends.", default=None
    )

    @field_validator("corp_filter", mode="before")
    def _check_corp_filter(cls, value, valid_values):
        if value is None or valid_values.data["corp_filter_col"] is None:
            raise ValueError(
                "Both of corp_filter and corp_filter_col should be defined."
            )
        return value

    def fetch_data(self, code, cut_date=None):
        """Fetch data via Yahoo finance."""
        income_statement = get_yfinance_income_statement(ticker=code)
        balance_sheet = get_yfinance_balance_sheet(ticker=code)
        cash_flow = get_yfinance_cash_flow(ticker=code)
        dividends = get_yfinance_dividends(ticker=code)

        if cut_date is not None:
            income_statement = income_statement[income_statement["date"] >= cut_date]
            balance_sheet = balance_sheet[balance_sheet["date"] >= cut_date]
            cash_flow = cash_flow[cash_flow["date"] >= cut_date]
            dividends = dividends[dividends["date"] >= cut_date]

        return income_statement, balance_sheet, cash_flow, dividends

    def run(self) -> None:
        """Execute workflow."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        # Get variables.
        v = self.variables
        # Get a logger.
        logger = self.logger

        if v["cut_date"] is None:
            cut_date = datetime.datetime.today() - datetime.timedelta(days=30)
            cut_date = cut_date.strftime("%Y-%m-%d")
        else:
            cut_date = v["cut_date"]

        income_statements = []
        balance_sheets = []
        cash_flows = []
        dividends_reports = []

        # If corp_codes is defined, use them; otherwise get all listed corp codes from JQuants.
        if v["corp_codes"]:
            for code in v["corp_codes"]:
                logger.info(f"Fetching data for code: {code}")
                logger.info(f"Data ingestion after {cut_date}.")
                income_statement, balance_sheet, cash_flow, dividends = self.fetch_data(
                    f"{code[:4]}.T", cut_date
                )
                income_statements.append(income_statement)
                balance_sheets.append(balance_sheet)
                cash_flows.append(cash_flow)
                dividends_reports.append(dividends)
                time.sleep(1)  # To avoid hitting rate limits.

            income_statements_df = pd.concat(income_statements, ignore_index=True)
            balance_sheets_df = pd.concat(balance_sheets, ignore_index=True)
            cash_flows_df = pd.concat(cash_flows, ignore_index=True)
            dividends_reports_df = pd.concat(dividends_reports, ignore_index=True)

        else:
            logger.info("Updting JQuants tokens.")
            _, id_token = update_jquants_tokens(save_as_file=True)
            logger.info("JQuants tokens updated successfully.")

            # Get corporation list from JQuants.
            corp_lists = get_jquants_corporate_list(idToken=id_token)
            # Apply corporation filter if defined.
            if v["corp_filter"]:
                corp_lists = corp_lists[
                    corp_lists[v["corp_filter_col"]]
                    .astype(str)
                    .str.contains(v["corp_filter"], regex=True, na=False)
                ]["Code"].tolist()
                logger.info(f"Total {len(corp_lists)} corporations after filtering.")
            else:
                corp_lists = corp_lists["Code"].tolist()

            for code in corp_lists:
                logger.info(f"Fetching data for code: {code}")
                logger.info(f"Data ingestion after {cut_date}.")
                income_statement, balance_sheet, cash_flow, dividends = self.fetch_data(
                    f"{code[:4]}.T", cut_date
                )
                income_statements.append(income_statement)
                balance_sheets.append(balance_sheet)
                cash_flows.append(cash_flow)
                dividends_reports.append(dividends)
                time.sleep(1)  # To avoid hitting rate limits.

            income_statements_df = pd.concat(income_statements, ignore_index=True)
            balance_sheets_df = pd.concat(balance_sheets, ignore_index=True)
            cash_flows_df = pd.concat(cash_flows, ignore_index=True)
            dividends_reports_df = pd.concat(dividends_reports, ignore_index=True)

        # Replace ".T" with "0" in ticker codes
        income_statements_df["ticker"] = income_statements_df["ticker"].str.replace(
            ".T", "0"
        )
        balance_sheets_df["ticker"] = balance_sheets_df["ticker"].str.replace(".T", "0")
        cash_flows_df["ticker"] = cash_flows_df["ticker"].str.replace(".T", "0")
        dividends_reports_df["ticker"] = dividends_reports_df["ticker"].str.replace(
            ".T", "0"
        )

        if (
            income_statements_df.empty
            or balance_sheets_df.empty
            or cash_flows_df.empty
            or dividends_reports_df.empty
        ):
            logger.warning("No data fetched from Yahoo finance API.")
            return

        if v["missing_col_impute"]:
            # Add missing columns based on schema.
            for col in v["schema_income_statement"]["schemas"].keys():
                if col not in income_statements_df.columns:
                    income_statements_df[col] = None
            for col in v["schema_balance_sheet"]["schemas"].keys():
                if col not in balance_sheets_df.columns:
                    balance_sheets_df[col] = None
            for col in v["schema_cash_flow"]["schemas"].keys():
                if col not in cash_flows_df.columns:
                    cash_flows_df[col] = None
            for col in v["schema_dividends"]["schemas"].keys():
                if col not in dividends_reports_df.columns:
                    dividends_reports_df[col] = None

        # Validate dataframe against schema.
        income_statements_df_vaid = Validator.model_validate(
            v["schema_income_statement"]
        ).valid(income_statements_df)
        balance_sheets_df_valid = Validator.model_validate(
            v["schema_balance_sheet"]
        ).valid(balance_sheets_df)
        cash_flows_df_valid = Validator.model_validate(v["schema_cash_flow"]).valid(
            cash_flows_df
        )
        dividends_reports_df_valid = Validator.model_validate(
            v["schema_dividends"]
        ).valid(dividends_reports_df)

        # Output
        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return (
                income_statements_df_vaid,
                balance_sheets_df_valid,
                cash_flows_df_valid,
                dividends_reports_df_valid,
            )
        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            if v["target_path_income_statement"]:
                SaveFile(
                    df=income_statements_df_vaid,
                    file_db=v["target_db"],
                    file_path=v["target_path_income_statement"],
                    primary_keys=extract_primary_keys(v["schema_income_statement"]),
                ).mode(v["write_mode"]).save()
            else:
                logger.error("No path defined for income statement!")

            if v["target_path_balance_sheet"]:
                SaveFile(
                    df=balance_sheets_df_valid,
                    file_db=v["target_db"],
                    file_path=v["target_path_balance_sheet"],
                    primary_keys=extract_primary_keys(v["schema_balance_sheet"]),
                ).mode(v["write_mode"]).save()
            else:
                logger.error("No path defined for balance sheet!")

            if v["target_path_cash_flow"]:
                SaveFile(
                    df=cash_flows_df_valid,
                    file_db=v["target_db"],
                    file_path=v["target_path_cash_flow"],
                    primary_keys=extract_primary_keys(v["schema_cash_flow"]),
                ).mode(v["write_mode"]).save()
            else:
                logger.error("No path defined for cash flow!")

            if v["target_path_dividends"]:
                SaveFile(
                    df=dividends_reports_df_valid,
                    file_db=v["target_db"],
                    file_path=v["target_path_dividends"],
                    primary_keys=extract_primary_keys(v["schema_dividends"]),
                ).mode(v["write_mode"]).save()
            else:
                logger.error("No path defined for dividends!")

            logger.info("Data saved to CSV format.")

        else:
            raise NotImplementedError("other type not implemented yet!")


class DataIngestionYFinanceStockPrice(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for annual data ingestion via Yahoo Finance.

    Description: Financial Report Data Ingestion via Yahoo Finance API.

    Key methods
    ----------
    run: Abstract method.
        Execute overall workflow. To be implemented at subclasses.
    logger: set up a logger for workflow.
    variables: Return variables of the workflow.
    ----------

    Variables
    ----------
    corp_codes: Corporate codes to ingest data for.
        List[str] = None
    corp_filter_col: Column name for corporation filtering.
        str = None
    corp_filter: Filter string for corporation filtering (using regex).
        str = None
    start_date: Start date for data ingestion in 'YYYY-MM-DD' format.
        str = None
    end_date: End date for data ingestion in 'YYYY-MM-DD' format.
        str = None
    period: If data ingestion done by a defined period.
        str = None (i.e. 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
    interval: If data ingestion done by a defined period, we can define the interval.
        str = None (i.e. 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
    format: Output format.
        str = "df"
    target_db: Folder or Database path to save the ingested data.
        Optional[str] = None
    target_path_stock: Full file path to save the stock price data.
        Optional[str] = None
    write_mode: File write mode.
        str = "overwrite"
    schema_stock: Output data schema of stock price.
        Optional[Dict] = Field(description="data_schema_stock_price.", default=None)
    ----------
    """

    # variables
    wfclss: ClassVar[str] = "DataIngestionYFinanceStockPrice"

    kind: str = "DataIngestionYFinanceStockPrice"
    corp_codes: List[str] = None
    corp_filter_col: str = None
    corp_filter: str = None
    start_date: str = None
    end_date: str = None
    period: str = None
    interval: str = None
    format: str = "df"
    target_db: Optional[str] = None
    target_path_stock: Optional[str] = None
    write_mode: str = "overwrite"
    schema_stock_price: Optional[Dict] = Field(
        description="data_schema_stock_price.", default=None
    )

    @field_validator("period", mode="before")
    def _check_period(cls, value, valid_values):
        if (
            valid_values.data["start_date"] is not None
            or valid_values.data["end_date"] is not None
        ) and value is not None:
            raise ValueError(
                "If start_date & end_date are defined, period should be None."
            )
        return value

    @field_validator("interval", mode="before")
    def _check_interval(cls, value, valid_values):
        if (
            valid_values.data["start_date"] is not None
            or valid_values.data["end_date"] is not None
        ) and value is not None:
            raise ValueError(
                "If start_date & end_date are defined, interval should be None."
            )
        return value

    @field_validator("corp_filter", mode="before")
    def _check_corp_filter(cls, value, valid_values):
        if value is None or valid_values.data["corp_filter_col"] is None:
            raise ValueError(
                "Both of corp_filter and corp_filter_col should be defined."
            )
        return value

    def fetch_data(
        self, codes, start_date=None, end_date=None, period=None, interval=None
    ):
        """Fetch data via Yahoo finance."""
        if start_date is not None and end_date is not None:
            stock_price = get_yfinance_stock_prices(
                tickers=codes,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            stock_price = get_yfinance_stock_prices(
                tickers=codes,
                period=period,
                interval=interval,
            )

        return stock_price

    def run(self) -> None:
        """Execute workflow."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        # Get variables.
        v = self.variables
        # Get a logger.
        logger = self.logger

        stock_prices = []

        if (
            v["start_date"] is None
            and v["end_date"] is None
            and v["period"] is None
            and v["interval"] is None
        ):
            logger.info(
                "No start_date/end_date or period/interval defined, setting period to '1d'."
            )
            v["period"] = "1d"

        # If corp_codes is defined, use them; otherwise get all listed corp codes from JQuants.
        if v["corp_codes"]:
            logger.info(f"Fetching data for code: {v['corp_codes']}")
            corp_lists = [f"{code[:4]}.T" for code in v["corp_codes"]]
            stock_price = self.fetch_data(
                corp_lists,
                v["start_date"],
                v["end_date"],
                v["period"],
                v["interval"],
            )
            stock_prices.append(stock_price)
            time.sleep(1)  # To avoid hitting rate limits.

            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        else:
            logger.info("Updting JQuants tokens.")
            _, id_token = update_jquants_tokens(save_as_file=True)
            logger.info("JQuants tokens updated successfully.")

            # Get corporation list from JQuants.
            corp_lists = get_jquants_corporate_list(idToken=id_token)
            # Apply corporation filter if defined.
            if v["corp_filter"]:
                corp_lists = corp_lists[
                    corp_lists[v["corp_filter_col"]]
                    .astype(str)
                    .str.contains(v["corp_filter"], regex=True, na=False)
                ]["Code"].tolist()
                logger.info(f"Total {len(corp_lists)} corporations after filtering.")
            else:
                corp_lists = [f"{code[:4]}.T" for code in corp_lists["Code"].tolist()]

            logger.info(f"Fetching data for code: {corp_lists}")

            stock_price = self.fetch_data(
                corp_lists,
                v["start_date"],
                v["end_date"],
                v["period"],
                v["interval"],
            )
            stock_prices.append(stock_price)
            time.sleep(1)  # To avoid hitting rate limits.

            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        if stock_prices_df.empty:
            logger.warning("No data fetched from Yahoo finance API.")
            return

        stock_prices_df["ticker"] = stock_prices_df["ticker"].str.replace(".T", "0")

        # Validate dataframe against schema.
        stock_prices_df_vaid = Validator.model_validate(v["schema_stock_price"]).valid(
            stock_prices_df
        )

        # Output
        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return stock_prices_df_vaid

        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            if v["target_path_stock"]:
                SaveFile(
                    df=stock_prices_df_vaid,
                    file_db=v["target_db"],
                    file_path=v["target_path_stock"],
                    primary_keys=extract_primary_keys(v["schema_stock_price"]),
                ).mode(v["write_mode"]).save()
            else:
                logger.error("No path defined for stock price!")

        else:
            raise NotImplementedError("other type not implemented yet!")
