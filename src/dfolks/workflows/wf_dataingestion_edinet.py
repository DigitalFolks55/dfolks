"""
Workflow for data ingestion from EDINET.

Need to work:
0) Brush up workflow; Need to update XBRL parser & validator.
1) Add more error handling.
2) Add file type; parquet.
3) For other data.
"""

import glob
import logging
import os
import tempfile
import time
from datetime import datetime
from typing import ClassVar, Dict, List, Optional

import pandas as pd
from arelle import Cntlr, ModelManager
from dateutil.relativedelta import relativedelta
from pydantic import Field

from dfolks.core.classfactory import WorkflowsRegistry
from dfolks.core.mixin import ExternalFileMixin
from dfolks.data.data import Validator, add_ingestion_metadata
from dfolks.data.edinet_apis import download_edinet_documents, get_edinet_document_list
from dfolks.data.jquants_apis import (
    get_jquants_api_key_v2,
    get_jquants_corporate_list_v2,
)
from dfolks.data.output import (
    SaveFile,
)
from dfolks.parsers.xbrlparser import EdinetXbrlParser
from dfolks.utils.utils import extract_primary_keys

# Set up shared logger
logger = logging.getLogger("shared")


class DataIngestionEdinet(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for data ingestion from EDINET.

    Description
    Download and Parse XBRL files from EDINET based on the specified date range and corporate codes.

    Key methods
    ----------
    run: Abstract method.
        Execute overall workflow. To be implemented at subclasses.
    logger: set up a logger for workflow.
    variables: Return variables of the workflow.
    ----------

    Variables
    status: Execute this workflow or not
        bool = True
    corp_codes: List of corporate codes to ingest data for.
        List[str] = None
    start_date: Start date for data ingestion in 'YYYY-MM-DD' format.
        str = None
    end_date: End date for data ingestion in 'YYYY-MM-DD' format.
        str = None
    temp_folder_path: Temporary folder path for storing downloaded files. None to use system temp folder.
        str = None
    format: Output format.
        str = "df"
    target_db: Folder or Database path to save the ingested data.
        Optional[str] = None
    target_path_fin_report: Full file path to save the financial report data.
        Optional[str] = None
    ingestion_source: Data source.
        str
    ----------
    """

    wfclss: ClassVar[str] = "DataIngestionEdinet"

    kind: str = "DataIngestionEdinet"
    status: bool = True
    corp_codes: List[str] = None
    corp_filter_col: str = None
    corp_filter: str = None
    start_date: str = None
    end_date: str = None
    temp_folder_path: str = None
    format: str = "df"
    target_db: Optional[str] = None
    target_path_fin_report: Optional[str] = None
    write_mode: str = "overwrite"
    schema_fin_report: Optional[Dict] = Field(description="data_schema.", default=None)
    ingestion_source: str

    def date_range(self, start_date, end_date):
        """Generate date range."""
        # If end_date is not provided, set it to today's date.
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        # If start_date is not provided, set it to 1 month before from today's date.
        if start_date is None:
            start_date = datetime.strptime(end_date, "%Y-%m-%d") - relativedelta(
                months=1
            )
            start_date = start_date.strftime("%Y-%m-%d")

        date_range = pd.date_range(start=start_date, end=end_date)

        return date_range

    def run(self) -> None:
        """Execute workflow."""
        # Get a logger.
        logger = self.logger
        logger.info("Starting data ingestion workflow for EDINET.")
        # Get variables.
        logger.info("Retrieving workflow variables.")
        v = self.variables

        doc_lists = []

        # Data range
        dates = self.date_range(start_date=v["start_date"], end_date=v["end_date"])
        logger.info(
            f"Data range set from {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}."
        )

        # If corp_codes is not defined, get all listed corp codes from JQuants.
        if v["corp_codes"] is None:
            logger.info("Get JQuants api key.")
            api_key = get_jquants_api_key_v2()
            logger.info("JQuants api key retrieved successfully.")
            corp_lists = get_jquants_corporate_list_v2(api_key=api_key)["Code"].tolist()
        else:
            logger.info("Using provided corporate codes for data ingestion.")
            corp_lists = v["corp_codes"]

        for date in dates:
            logger.info(f"Fetching documents for date: {date}.")
            docs = get_edinet_document_list(date=date)

            # Fetch document list for the date.
            if docs is None or len(docs) == 0:
                logger.info(f"No documents found for date: {date}")
                time.sleep(1)  # To avoid hitting API rate limits.
                continue
            else:
                logger.info(f"Total {len(docs)} documents found for date: {date}.")
                logger.info(
                    "Filtering annual documents for predefined corporate codes."
                )
                doc_lists.append(
                    docs[
                        (docs["ordinanceCode"] == "010")  # Variable?
                        & (docs["formCode"] == "030000")  # Variable?
                        & (docs["secCode"].isin(corp_lists))
                    ]
                )
                time.sleep(1)  # To avoid hitting API rate limits.

        # Combine document lists.
        logger.info("Combining documents into one dataframe")
        doc_lists = pd.concat(doc_lists, ignore_index=True)

        # Download EDINET XBRL files and parse them.
        # If temp_folder_path is None, use system temp folder.
        if v["temp_folder_path"] is None:
            logger.info("Using system temporary folder for downloading files.")
            with tempfile.TemporaryDirectory() as temp_folder_path:
                logger.info(
                    f"Temporary folder created at {temp_folder_path} and data will be downloaded there."
                )
                download_edinet_documents(
                    doc_list=doc_lists, folder_path=temp_folder_path
                )

                # Grab all XBRL files in the temp folder.
                xbrl_files = glob.glob(
                    os.path.join(temp_folder_path, "*/XBRL/PublicDoc/*.xbrl"),
                    recursive=True,
                )

                logger.info(f"Total {len(xbrl_files)} XBRL files found for parsing.")

                dfs = []

                # Parse each XBRL file.
                for file in xbrl_files:
                    logger.info(f"Parsing file: {file}")
                    ctrl = Cntlr.Cntlr()
                    model_manager = ModelManager.initialize(ctrl)
                    model_xbrl = model_manager.load(file)

                    parser = EdinetXbrlParser(model_xbrl=model_xbrl, source_path=file)

                    dfs.append(parser.parse())
                    logger.info(f"Finished parsing file: {file}")

                logger.info("Combining parsed data into one DataFrame.")
                dfs = pd.concat(dfs, ignore_index=True)

        # Else use defined temp_folder_path and retain downloaded files.
        else:
            logger.info(f"Using defined temporary folder: {v['temp_folder_path']}")
            download_edinet_documents(
                doc_list=doc_lists, folder_path=v["temp_folder_path"]
            )

            # Grab all XBRL files in the temp folder.
            xbrl_files = glob.glob(
                os.path.join(v["temp_folder_path"], "*/XBRL/PublicDoc/*.xbrl"),
                recursive=True,
            )

            logger.info(f"Total {len(xbrl_files)} XBRL files found for parsing.")

            dfs = []

            # Parse each XBRL file.
            for file in xbrl_files:
                logger.info(f"Parsing file: {file}")
                ctrl = Cntlr.Cntlr()
                model_manager = ModelManager.initialize(ctrl)
                model_xbrl = model_manager.load(file)

                parser = EdinetXbrlParser(model_xbrl=model_xbrl, source_path=file)

                dfs.append(parser.parse())
                logger.info(f"Finished parsing file: {file}")

            logger.info("Combining parsed data into one DataFrame.")
            dfs = pd.concat(dfs, ignore_index=True)

        # Add metadata of data ingestion.
        dfs = add_ingestion_metadata(dfs, v["ingestion_source"])

        # Validate dataframe against schema.
        logger.info("Apply dataframe validator for parsed dataframe")
        df_valid = Validator.model_validate(v["schema_fin_report"]).valid(dfs)

        # Output results based on the defined format.
        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return df_valid
        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            SaveFile(
                df=df_valid,
                file_type=v["format"],
                file_db=v["target_db"],
                file_path=v["target_path_fin_report"],
                primary_keys=extract_primary_keys(v["schema_fin_report"]),
            ).mode(v["write_mode"]).save()

            logger.info("Data saved to CSV format.")

        else:
            raise NotImplementedError("Other type not implemented yet!")
