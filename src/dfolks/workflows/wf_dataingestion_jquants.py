"""
Workflow for data ingestion from JQuants.

Need to work:
0) Time duration with start & end date.
    - if no start & end date, then last 1 month
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
    get_jquants_fin_report,
    get_jquants_stock_price,
    update_jquants_tokens,
)
from dfolks.data.output import SaveFile

# Set up shared logger
logger = logging.getLogger("shared")


class DataIngestionJQuantsFinReport(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for data ingestion from JQuants.

    Description

    Key methods
    ----------
    run: Abstract method.
        Execute overall workflow. To be implemented at subclasses.
    logger: set up a logger for workflow.
    variables: Return variables of the workflow.
    ----------

    Variables
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
    format: str = "df"
    target_db: Optional[str] = None
    target_path_fin_report: Optional[str] = None
    write_mode: str = "overwrite"
    write_options: Dict = Field(description="write_option", default=None)
    schema: Optional[Dict] = Field(description="data_schema.", default=None)

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

    @field_validator("corp_filter", mode="before")
    def _check_corp_filter(cls, value, valid_values):
        if value is None or valid_values.data["corp_filter_col"] is None:
            raise ValueError(
                "Both of corp_filter and corp_filter_col should be defined."
            )
        return value

    def fetch_data(self, idToken, code, date=None):
        """Fetch data from JQuants."""
        # Implementation for fetching data from JQuants
        fin_report = get_jquants_fin_report(idToken=idToken, code=code, date=date)

        return fin_report

    def run(self) -> None:
        """Run."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        v = self.variables
        logger = self.logger

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
        elif (
            v["start_date"] is None
            and v["end_date"] is None
            and v["single_date"] is None
        ):
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

        logger.info("Updting JQuants tokens.")
        _, id_token = update_jquants_tokens(save_as_file=True)
        logger.info("JQuants tokens updated successfully.")

        if v["corp_codes"]:
            for code in v["corp_codes"]:
                logger.info(f"Fetching data for code: {code}")

                if v["single_date"]:
                    logger.info(f"Data ingestion for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        fin_report = self.fetch_data(id_token, code, None)
                    else:
                        fin_report = self.fetch_data(id_token, code, v["single_date"])
                    fin_reports.append(fin_report)
                else:
                    for date in date_range:
                        logger.info(f"Data ingestion for {date}.")
                        fin_report = self.fetch_data(id_token, code, date)
                        fin_reports.append(fin_report)
                        time.sleep(1)
                time.sleep(1)  # To avoid hitting rate limits

            fin_reports_df = pd.concat(fin_reports, ignore_index=True)

        else:
            corp_lists = get_jquants_corporate_list(idToken=id_token)
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
                if v["single_date"]:
                    logger.info(f"Data ingestion for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        fin_report = self.fetch_data(id_token, code, None)
                    else:
                        fin_report = self.fetch_data(id_token, code, v["single_date"])
                    fin_reports.append(fin_report)
                else:
                    for date in date_range:
                        logger.info(f"Data ingestion for {date}.")
                        fin_report = self.fetch_data(id_token, code, date)
                        fin_reports.append(fin_report)
                        time.sleep(1)
                time.sleep(1)  # To avoid hitting rate limits

            fin_reports_df = pd.concat(fin_reports, ignore_index=True)

        if fin_reports_df.empty:
            logger.warning("No data fetched from JQuants Fin Report API.")
            return

        df_valid = Validator.model_validate(v["schema"]).valid(fin_reports_df)

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
                ).mode("overwrite").save()
            logger.info("Data saved to CSV format.")
            if not v["target_path_fin_report"]:
                logger.error("No path defined!")


class DataIngestionJQuantsStockPrice(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for data ingestion from JQuants.

    Description

    Key methods
    ----------
    run: Abstract method.
        Execute overall workflow. To be implemented at subclasses.
    logger: set up a logger for workflow.
    variables: Return variables of the workflow.
    ----------

    Variables
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
    format: str = "df"
    target_db: Optional[str] = None
    target_path_stock: Optional[str] = None
    write_mode: str = "overwrite"
    write_options: Dict = Field(description="write_option", default=None)
    schema: Optional[Dict] = Field(description="data_schema.", default=None)

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

    @field_validator("corp_filter", mode="before")
    def _check_corp_filter(cls, value, valid_values):
        if value is None or valid_values.data["corp_filter_col"] is None:
            raise ValueError(
                "Both of corp_filter and corp_filter_col should be defined."
            )
        return value

    def fetch_data(self, idToken, code, date=None, start_date=None, end_date=None):
        """Fetch data from JQuants."""
        # Implementation for fetching data from JQuants
        stock_price = get_jquants_stock_price(
            idToken=idToken,
            code=code,
            date=date,
            date_from=start_date,
            date_to=end_date,
        )

        return stock_price

    def run(self) -> None:
        """Run."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        v = self.variables
        logger = self.logger

        if v["start_date"] and v["end_date"]:
            logger.info(f"Data ingestion from {v['start_date']} to {v['end_date']}.")
            start_date = v["start_date"]
            end_date = v["end_date"]
        elif (
            v["start_date"] is None
            and v["end_date"] is None
            and v["single_date"] is None
        ):
            logger.info("Data ingestion for yesterday.")
            v["single_date"] = datetime.date.today() - datetime.timedelta(days=1)

        stock_prices = []

        logger.info("Updting JQuants tokens.")
        _, id_token = update_jquants_tokens(save_as_file=True)
        logger.info("JQuants tokens updated successfully.")

        if v["corp_codes"]:
            for code in v["corp_codes"]:
                logger.info(f"Fetching data for code: {code}")
                if v["single_date"]:
                    logger.info(f"Fetching data for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        stock_price = self.fetch_data(id_token, code, date=None)
                    else:
                        stock_price = self.fetch_data(
                            id_token, code, date=v["single_date"]
                        )
                    stock_prices.append(stock_price)
                else:
                    logger.info(f"Fetching data from {start_date} to {end_date}.")
                    stock_price = self.fetch_data(
                        id_token, code, start_date=start_date, end_date=end_date
                    )
                    stock_prices.append(stock_price)
                    time.sleep(1)
                time.sleep(1)  # To avoid hitting rate limits

            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        else:
            corp_lists = get_jquants_corporate_list(idToken=id_token)
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
                if v["single_date"]:
                    logger.info(f"Fetching data for {v["single_date"]}.")
                    if v["single_date"] == "whole":
                        stock_price = self.fetch_data(id_token, code, date=None)
                    else:
                        stock_price = self.fetch_data(
                            id_token, code, date=v["single_date"]
                        )
                    stock_prices.append(stock_price)
                else:
                    logger.info(f"Fetching data from {start_date} to {end_date}.")
                    stock_price = self.fetch_data(
                        id_token, code, start_date=start_date, end_date=end_date
                    )
                    stock_prices.append(stock_price)
                    time.sleep(1)
                time.sleep(1)  # To avoid hitting rate limits

            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        df_valid = Validator.model_validate(v["schema"]).valid(stock_prices_df)

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
                ).mode("overwrite").save()
            logger.info("Data saved to CSV format.")
            if not v["target_path_fin_report"]:
                logger.error("No path defined!")
