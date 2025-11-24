"""
Workflow for data ingestion from JQuants.

Need to work:
0) TBD
"""

import glob
import logging
import os
import tempfile
import time
from datetime import datetime
from typing import ClassVar, List, Optional

import pandas as pd
from arelle import Cntlr, ModelManager
from dateutil.relativedelta import relativedelta

from dfolks.core.classfactory import WorkflowsRegistry
from dfolks.data.edinet_apis import download_edinet_documents, get_edinet_document_list
from dfolks.data.jquants_apis import (
    get_jquants_corporate_list,
    update_jquants_tokens,
)
from dfolks.data.output import (
    SaveFile,
)
from dfolks.parsers.xbrlparser import EdinetXbrlParser

# Set up shared logger
logger = logging.getLogger("shared")


class DataIngestionEdinet(WorkflowsRegistry):
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
    wfclss: ClassVar[str] = "DataIngestionEdinet"

    corp_codes: List[str] = None
    start_date: str = None
    end_date: str = None
    temp_folder_path: str = None
    format: str = "df"
    target_db: Optional[str] = None
    target_path_fin_report: Optional[str] = None

    def date_range(self, start_date, end_date):
        if end_date is None:
            "1month before"
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = datetime.strptime(end_date, "%Y-%m-%d") - relativedelta(
                months=1
            )
            start_date = start_date.strftime("%Y-%m-%d")

        date_range = pd.date_range(start=start_date, end=end_date)

        return date_range

    def run(self) -> None:
        """Run."""
        self.logger.info("Starting data ingestion workflow for Edinet.")
        v = self.variables
        logger = self.logger

        doc_lists = []

        dates = self.date_range(start_date=v["start_date"], end_date=v["end_date"])

        if v["corp_codes"] is None:
            logger.info("Updting JQuants tokens.")
            _, id_token = update_jquants_tokens(save_as_file=True)
            logger.info("JQuants tokens updated successfully.")
            corp_lists = get_jquants_corporate_list(idToken=id_token)["Code"].tolist()
        else:
            corp_lists = v["corp_codes"]

        for date in dates:
            logger.info(f"Fetching documents for date: {date}.")
            docs = get_edinet_document_list(date=date)

            if docs is None or len(docs) == 0:
                logger.info(f"No documents found for date: {date}")
                time.sleep(1)
                continue
            else:
                doc_lists.append(
                    docs[
                        (docs["ordinanceCode"] == "010")
                        & (docs["formCode"] == "030000")
                        & (docs["secCode"].isin(corp_lists))
                    ]
                )
                time.sleep(1)

        doc_lists = pd.concat(doc_lists, ignore_index=True)

        if v["temp_folder_path"] is None:
            with tempfile.TemporaryDirectory() as temp_folder_path:
                download_edinet_documents(
                    doc_list=doc_lists, folder_path=temp_folder_path
                )

                xbrl_files = glob.glob(
                    os.path.join(
                        temp_folder_path, "/*/XBRL/PublicDoc/*.xbrl", recursive=True
                    )
                )

                dfs = []

                for file in xbrl_files:
                    ctrl = Cntlr.Cntlr()
                    model_manager = ModelManager.initialize(ctrl)
                    model_xbrl = model_manager.load(file)

                    parser = EdinetXbrlParser(model_xbrl=model_xbrl)

                    dfs.append(parser.parse())

                dfs = pd.concat(dfs, ignore_index=True)

        else:
            download_edinet_documents(
                doc_list=doc_lists, folder_path=v["temp_folder_path"]
            )

            xbrl_files = glob.glob(
                os.path.join(v["temp_folder_path"], "*/XBRL/PublicDoc/*.xbrl"),
                recursive=True,
            )

            dfs = []

            for file in xbrl_files:
                logger.info(f"Parsing file: {file}")
                ctrl = Cntlr.Cntlr()
                model_manager = ModelManager.initialize(ctrl)
                model_xbrl = model_manager.load(file)

                parser = EdinetXbrlParser(model_xbrl=model_xbrl)

                dfs.append(parser.parse())

            dfs = pd.concat(dfs, ignore_index=True)

        if v["format"] == "df":
            logger.info("Returning DataFrame format.")
            return dfs
        elif v["format"] == "csv":
            logger.info("Saving DataFrame to CSV format.")
            SaveFile(
                df=dfs,
                file_type=v["format"],
                file_db=v["target_db"],
                file_path=v["target_path_fin_report"],
            ).mode("overwrite").save()

            logger.info("Data saved to CSV format.")
