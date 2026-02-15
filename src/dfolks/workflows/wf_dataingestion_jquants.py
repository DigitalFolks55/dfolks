"""
Workflow for data ingestion from JQuants.

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
    get_jquants_api_key_v2,
    get_jquants_corporate_list_v2,
    get_jquants_fin_report_v2,
    get_jquants_industry_report_v2,
    get_jquants_stock_price_v2,
)
from dfolks.data.output import SaveFile
from dfolks.utils.utils import extract_primary_keys

# Set up shared logger
logger = logging.getLogger("shared")


class DataIngestionJQuantsFinReport(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for data ingestion from JQuants.

    Description: Financial Report Data Ingestion from JQuants API.

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
    single_date: If single date ingesiton, use this date in 'YYYY-MM-DD' format.
        str = None
    format: Output format.
        str = "df"
    target_db: Folder or Database path to save the ingested data.
        Optional[str] = None
    target_path_fin_report: Full file path to save the financial report data.
        Optional[str] = None
    write_mode: File write mode.
        str = "overwrite"
    schema: Output data schema.
        Optional[Dict] = Field(description="data_schema.", default=None)
    ----------
    """

    # variables
    wfclss: ClassVar[str] = "DataIngestionJQuantsFinReport"

    kind: str = "DataIngestionJQuantsFinReport"
    corp_codes: List[str] = None
    corp_filter_col: str = None
    corp_filter: str = None
    start_date: str = None
    end_date: str = None
    single_date: str = None
    latest_data: bool = False
    format: str = "df"
    target_db: Optional[str] = None
    target_path_fin_report: Optional[str] = None
    write_mode: str = "overwrite"
    schema_fin_report: Optional[Dict] = Field(description="data_schema.", default=None)

    @field_validator("single_date", mode="before")
    def _check_single_date(cls, value, valid_values):
        if (
            valid_values.data["start_date"] is not None
            or valid_values.data["end_date"] is not None
        ) and value is not None:
            raise ValueError(
                "If start_date & end_date are defined, single_date should be None."
            )
        return value

    @field_validator("latest_data", mode="before")
    def _check_latest_data(cls, value, valid_values):
        if (
            valid_values.data["start_date"] is not None
            or valid_values.data["end_date"] is not None
            or valid_values.data["single_date"] is not None
        ) and value is True:
            raise ValueError(
                "If start_date, end_date & single_date are defined, latest_data should be False."
            )
        return value

    @field_validator("corp_filter", mode="before")
    def _check_corp_filter(cls, value, valid_values):
        if value is None or valid_values.data["corp_filter_col"] is None:
            raise ValueError(
                "Both of corp_filter and corp_filter_col should be defined."
            )
        return value

    def fetch_data(self, api_key, code, date=None):
        """Fetch data from JQuants."""
        # Implementation for fetching data from JQuants.
        fin_report = get_jquants_fin_report_v2(api_key=api_key, code=code, date=date)

        return fin_report

    def run(self) -> None:
        """Execute workflow."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        # Get variables.
        v = self.variables
        # Get a logger.
        logger = self.logger

        # If start_date & end_date are defined, generate date range.
        if v["start_date"] and v["end_date"]:
            logger.info(f"Data ingestion from {v['start_date']} to {v['end_date']}.")
            start_date = pd.to_datetime(v["start_date"])
            end_date = pd.to_datetime(v["end_date"])
            date_range = (
                pd.date_range(start=start_date, end=end_date, freq="D")
                .to_series()
                .dt.strftime("%Y-%m-%d")
                .tolist()
            )
        # If latest_date is True, set date range to last 1 month.
        elif v["latest_data"] is True:
            logger.info("Data ingestion for last 1 month.")
            end_date = datetime.date.today()
            start_date = end_date - datetime.timedelta(days=30)
            date_range = (
                pd.date_range(start=start_date, end=end_date, freq="D")
                .to_series()
                .dt.strftime("%Y-%m-%d")
                .tolist()
            )

        fin_reports = []

        logger.info("Get JQuants api key.")
        api_key = get_jquants_api_key_v2()
        logger.info("JQuants api key updated successfully.")

        # If corp_codes is defined, use them; otherwise get all listed corp codes from JQuants.
        if v["corp_codes"]:
            for code in v["corp_codes"]:
                logger.info(f"Fetching data for code: {code}")

                # If single_date is defined, use it; otherwise use date_range.
                if v["single_date"]:
                    logger.info(f"Data ingestion for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        fin_report = self.fetch_data(api_key, code, None)
                    else:
                        fin_report = self.fetch_data(api_key, code, v["single_date"])
                    fin_reports.append(fin_report)
                else:
                    for date in date_range:
                        logger.info(f"Data ingestion for {date}.")
                        fin_report = self.fetch_data(api_key, code, date)
                        fin_reports.append(fin_report)
                        time.sleep(1)  # To avoid hitting rate limits.
                time.sleep(1)  # To avoid hitting rate limits.

            fin_reports_df = pd.concat(fin_reports, ignore_index=True)

        else:
            corp_lists = get_jquants_corporate_list_v2(api_key=api_key)
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
                # If single_date is defined, use it; otherwise use date_range.
                if v["single_date"]:
                    logger.info(f"Data ingestion for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        fin_report = self.fetch_data(api_key, code, None)
                    else:
                        fin_report = self.fetch_data(api_key, code, v["single_date"])
                    fin_reports.append(fin_report)
                else:
                    for date in date_range:
                        logger.info(f"Data ingestion for {date}.")
                        fin_report = self.fetch_data(api_key, code, date)
                        fin_reports.append(fin_report)
                        time.sleep(1)  # To avoid hitting rate limits.
                time.sleep(1)  # To avoid hitting rate limits.

            fin_reports_df = pd.concat(fin_reports, ignore_index=True)

        if fin_reports_df.empty:
            logger.warning("No data fetched from JQuants Fin Report API.")
            return

        # Validate dataframe against schema.
        df_valid = Validator.model_validate(v["schema_fin_report"]).valid(
            fin_reports_df
        )

        # Output
        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return df_valid
        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            if v["target_path_fin_report"]:
                SaveFile(
                    df=df_valid,
                    file_db=v["target_db"],
                    file_path=v["target_path_fin_report"],
                    primary_keys=extract_primary_keys(v["schema_fin_report"]),
                ).mode(v["write_mode"]).save()
            logger.info("Data saved to CSV format.")
            if not v["target_path_fin_report"]:
                logger.error("No path defined!")

        else:
            raise NotImplementedError("other type not implemented yet!")


class DataIngestionJQuantsStockPrice(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for data ingestion from JQuants.

    Description: Stock Price Data Ingestion from JQuants API.

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
    single_date: If single date ingesiton, use this date in 'YYYY-MM-DD' format.
        str = None
    format: Output format.
        str = "df"
    target_db: Folder or Database path to save the ingested data.
        Optional[str] = None
    target_path_fin_report: Full file path to save the financial report data.
        Optional[str] = None
    write_mode: File write mode.
        str = "overwrite"
    schema: Output data schema.
        Optional[Dict] = Field(description="data_schema.", default=None)
    ----------
    """

    # variables
    wfclss: ClassVar[str] = "DataIngestionJQuantsStockPrice"

    kind: str = "DataIngestionJQuantsStockPrice"
    corp_codes: List[str] = None
    corp_filter_col: str = None
    corp_filter: str = None
    start_date: str = None
    end_date: str = None
    single_date: str = None
    latest_data: bool = False
    format: str = "df"
    target_db: Optional[str] = None
    target_path_stock: Optional[str] = None
    write_mode: str = "overwrite"
    schema_stock_price: Optional[Dict] = Field(description="data_schema.", default=None)

    @field_validator("single_date", mode="before")
    def _check_single_date(cls, value, valid_values):
        if (
            valid_values.data["start_date"] is not None
            and valid_values.data["end_date"] is not None
        ) and value is not None:
            raise ValueError(
                "If start_date & end_date are defined, single_date should be None."
            )
        return value

    @field_validator("latest_data", mode="before")
    def _check_latest_data(cls, value, valid_values):
        if (
            valid_values.data["start_date"] is not None
            or valid_values.data["end_date"] is not None
            or valid_values.data["single_date"] is not None
        ) and value is True:
            raise ValueError(
                "If start_date, end_date & single_date are defined, latest_data should be False."
            )
        return value

    @field_validator("corp_filter", mode="before")
    def _check_corp_filter(cls, value, valid_values):
        if value is None or valid_values.data["corp_filter_col"] is None:
            raise ValueError(
                "Both of corp_filter and corp_filter_col should be defined."
            )
        return value

    def fetch_data(self, api_key, code, date=None, start_date=None, end_date=None):
        """Fetch data from JQuants."""
        # Implementation for fetching data from JQuants.
        stock_price = get_jquants_stock_price_v2(
            api_key=api_key,
            code=code,
            date=date,
            date_from=start_date,
            date_to=end_date,
        )

        return stock_price

    def run(self) -> None:
        """Execute workflow."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        # Get variables.
        v = self.variables
        # Get a logger.
        logger = self.logger

        # If start_date & end_date are defined, generate date range.
        if v["start_date"] and v["end_date"]:
            logger.info(f"Data ingestion from {v['start_date']} to {v['end_date']}.")
            start_date = v["start_date"]
            end_date = v["end_date"]
        # If neither start_date nor end_date is defined, set date range to yesterday.
        elif v["latest_data"] is True:
            logger.info("Data ingestion for yesterday.")
            v["single_date"] = datetime.date.today() - datetime.timedelta(days=1)

        stock_prices = []

        logger.info("Get JQuants api key.")
        api_key = get_jquants_api_key_v2()
        logger.info("JQuants api key updated successfully.")

        # If corp_codes is defined, use them; otherwise get all listed corp codes from JQuants.
        if v["corp_codes"]:
            for code in v["corp_codes"]:
                logger.info(f"Fetching data for code: {code}")
                # If single_date is defined, use it; otherwise use date_range.
                if v["single_date"]:
                    logger.info(f"Fetching data for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        stock_price = self.fetch_data(api_key, code, date=None)
                    else:
                        stock_price = self.fetch_data(
                            api_key, code, date=v["single_date"]
                        )
                    stock_prices.append(stock_price)
                else:
                    logger.info(f"Fetching data from {start_date} to {end_date}.")
                    stock_price = self.fetch_data(
                        api_key, code, start_date=start_date, end_date=end_date
                    )
                    stock_prices.append(stock_price)
                    time.sleep(1)  # To avoid hitting rate limits.
                time.sleep(1)  # To avoid hitting rate limits.

            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        else:
            corp_lists = get_jquants_corporate_list_v2(api_key=api_key)
            # Apply corporation filter if defined.
            if v["corp_filter"]:
                corp_lists = corp_lists[
                    corp_lists[v["corp_filter_col"]].astype(str)
                    # .str.contains(v["corp_filter"], regex=True, na=False)
                    .str.match(v["corp_filter"], na=False)
                ]["Code"].tolist()
                logger.info(f"Total {len(corp_lists)} corporations after filtering.")
            else:
                corp_lists = corp_lists["Code"].tolist()

            for code in corp_lists:
                logger.info(f"Fetching data for code: {code}")
                # If single_date is defined, use it; otherwise use date_range.
                if v["single_date"]:
                    logger.info(f"Fetching data for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        stock_price = self.fetch_data(api_key, code, date=None)
                    else:
                        stock_price = self.fetch_data(
                            api_key, code, date=v["single_date"]
                        )
                    stock_prices.append(stock_price)
                else:
                    logger.info(f"Fetching data from {start_date} to {end_date}.")
                    stock_price = self.fetch_data(
                        api_key, code, start_date=start_date, end_date=end_date
                    )
                    stock_prices.append(stock_price)
                    time.sleep(1)  # To avoid hitting rate limits.
                time.sleep(1)  # To avoid hitting rate limits.

            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        if stock_prices_df.empty:
            logger.warning("No data fetched from JQuants Stock Price API.")
            return

        # Validate dataframe against schema.
        df_valid = Validator.model_validate(v["schema_stock_price"]).valid(
            stock_prices_df
        )

        # Output
        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return df_valid
        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            if v["target_path_stock"]:
                SaveFile(
                    df=df_valid,
                    file_db=v["target_db"],
                    file_path=v["target_path_stock"],
                    primary_keys=extract_primary_keys(v["schema_stock_price"]),
                ).mode(v["write_mode"]).save()
            logger.info("Data saved to CSV format.")

            if not v["target_path_stock"]:
                logger.error("No path defined!")

        else:
            raise NotImplementedError("other type not implemented yet!")


class DataIngestionJQuantsIndustryReport(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for data ingestion from JQuants.

    Description: Financial Report Data Ingestion from JQuants API.

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
    single_date: If single date ingesiton, use this date in 'YYYY-MM-DD' format.
        str = None
    format: Output format.
        str = "df"
    target_db: Folder or Database path to save the ingested data.
        Optional[str] = None
    target_path_fin_report: Full file path to save the financial report data.
        Optional[str] = None
    write_mode: File write mode.
        str = "overwrite"
    schema: Output data schema.
        Optional[Dict] = Field(description="data_schema.", default=None)
    ----------
    """

    # variables
    wfclss: ClassVar[str] = "DataIngestionJQuantsIndustryReport"

    kind: str = "DataIngestionJQuantsIndustryReport"
    section: str = None
    start_date: str = None
    end_date: str = None
    latest_data: bool = False
    format: str = "df"
    target_db: Optional[str] = None
    target_path_industry_report: Optional[str] = None
    write_mode: str = "overwrite"
    schema_industry_report: Optional[Dict] = Field(
        description="data_schema.", default=None
    )

    @field_validator("latest_data", mode="before")
    def _check_latest_data(cls, value, valid_values):
        if (
            valid_values.data["start_date"] is not None
            and valid_values.data["end_date"] is not None
        ) and value is True:
            raise ValueError(
                "If start_date & end_date are defined, latest_data should be False vise versa."
            )
        return value

    def fetch_data(self, api_key, section, date_from, date_to):
        """Fetch data from JQuants."""
        # Implementation for fetching data from JQuants.
        industry_report = get_jquants_industry_report_v2(
            api_key=api_key,
            section=section,
            date_from=date_from,
            date_to=date_to,
        )

        return industry_report

    def run(self) -> None:
        """Execute workflow."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        # Get variables.
        v = self.variables
        # Get a logger.
        logger = self.logger

        # If neither start_date nor end_date is defined, set date range to last 1 week.
        if v["latest_data"]:
            logger.info("Data ingestion for last 7days.")
            end_date = datetime.date.today()
            start_date = end_date - datetime.timedelta(days=7)
        else:
            start_date = v["start_date"]
            end_date = v["end_date"]

        logger.info("Get JQuants api key.")
        api_key = get_jquants_api_key_v2()
        logger.info("JQuants api key updated successfully.")

        # If corp_codes is defined, use them; otherwise get all listed corp codes from JQuants.
        industry_report_df = self.fetch_data(
            api_key, v["section"], start_date, end_date
        )

        if industry_report_df.empty:
            logger.warning("No data fetched from JQuants Industry Report API.")
            return

        # Validate dataframe against schema.
        df_valid = Validator.model_validate(v["schema_industry_report"]).valid(
            industry_report_df
        )

        # Output
        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return df_valid
        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            if v["target_path_industry_report"]:
                SaveFile(
                    df=df_valid,
                    file_db=v["target_db"],
                    file_path=v["target_path_industry_report"],
                    primary_keys=extract_primary_keys(v["schema_industry_report"]),
                ).mode(v["write_mode"]).save()
            logger.info("Data saved to CSV format.")
            if not v["target_path_industry_report"]:
                logger.error("No path defined!")

        else:
            raise NotImplementedError("other type not implemented yet!")
