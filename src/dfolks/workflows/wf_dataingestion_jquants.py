"""
Workflow for data ingestion from JQuants.

Need to work:
0) TBD
"""

import logging
import time
from typing import ClassVar, List, Optional

import pandas as pd

from dfolks.core.classfactory import WorkflowsRegistry
from dfolks.data.jquants_apis import (
    get_jquants_corporate_list,
    get_jquants_fin_report,
    get_jquants_stock_price,
    update_jquants_tokens,
)
from dfolks.data.output import (
    SaveFile,
)

# Set up shared logger
logger = logging.getLogger("shared")


class DataIngestionJQuants(WorkflowsRegistry):
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
    wfclss: ClassVar[str] = "DataIngestionJQuants"

    corp_codes: List[str] = None
    format: str = "df"
    target_db: Optional[str] = None
    target_path_fin_report: Optional[str] = None
    target_path_stock: Optional[str] = None

    def fetch_data(self, idToken, code):
        """Fetch data from JQuants."""
        # Implementation for fetching data from JQuants
        fin_report = get_jquants_fin_report(idToken=idToken, code=code)
        stock_price = get_jquants_stock_price(idToken=idToken, code=code)

        return fin_report, stock_price

    def run(self) -> None:
        """Run."""
        self.logger.info("Starting data ingestion workflow for JQuants.")
        v = self.variables
        logger = self.logger

        fin_reports = []
        stock_prices = []

        logger.info("Updting JQuants tokens.")
        _, id_token = update_jquants_tokens(save_as_file=True)
        logger.info("JQuants tokens updated successfully.")

        if v["corp_codes"]:
            for code in v["corp_codes"]:
                logger.info(f"Fetching data for code: {code}")
                fin_report, stock_price = self.fetch_data(id_token, code)
                fin_reports.append(fin_report)
                stock_prices.append(stock_price)
                time.sleep(1)  # To avoid hitting rate limits

            fin_reports_df = pd.concat(fin_reports, ignore_index=True)
            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        else:
            corp_lists = get_jquants_corporate_list(idToken=id_token)
            for code in corp_lists:
                logger.info(f"Fetching data for code: {code}")
                fin_report, stock_price = self.fetch_data(id_token, code)
                fin_reports.append(fin_report)
                stock_prices.append(stock_price)
                time.sleep(1)  # To avoid hitting rate limits

            fin_reports_df = pd.concat(fin_reports, ignore_index=True)
            stock_prices_df = pd.concat(stock_prices, ignore_index=True)

        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return fin_reports_df, stock_prices_df
        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            if v["target_path_fin_report"]:
                SaveFile(
                    df=fin_reports_df,
                    file_db=v["target_db"],
                    file_path=v["target_path_fin_report"],
                ).mode("overwrite").save()
            if v["target_path_stock"]:
                SaveFile(
                    df=fin_reports_df,
                    file_db=v["target_db"],
                    file_path=v["target_path_stock"],
                ).mode("overwrite").save()
            logger.info("Data saved to CSV format.")
            if not v["target_path_fin_report"] and not v["target_path_stock"]:
                logger.error("No path defined!")
