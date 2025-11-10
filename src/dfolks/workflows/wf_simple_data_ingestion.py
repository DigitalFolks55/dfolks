"""Main workflow for data ingestion.

This workflow provide a data ingestion process.
Data processing, like a standardscaler, can be defined as a chain process; pre/post.
DataFrameValidator can be used for data validation with Pandera.
Output supports a DataFrame, a flat file, or a parquet file as of now; will be extended to other formats.

Need to do:
1) More data format for db solutions
2) Documentation
"""

from typing import ClassVar, Dict, List, Literal, Optional

from pydantic import Field

from dfolks.core.chain import ChainProcess
from dfolks.core.classfactory import WorkflowsRegistry, load_class
from dfolks.core.mixin import ExternalFileMixin
from dfolks.data.data import Validator


class DataIngestionWorkflow(WorkflowsRegistry, ExternalFileMixin):
    """Workflow for data ingestion.

    Variables
    ----------
    format: Literal["df", "csv", "parquet"]
        Output format - df, csv, parquet.
    target_db: Optional[str] = None
        Output database folder if the output is a flat file or a parquet file.
    target_output: Optional[str] = None
        Output file name if the output is a flat file or a parquet file.
    compression: Optional[str] = None
        Compression method for a flat or a parquet file.
    partition_cols: Optional[list] = None
        Partition columns for a parquet file. # for table in future
    parser: Dict = Field(description="parser.")
        Parser class for raw data extraction.
        Refer to the parser class for required variables.
    retain_cols: Optional[List] = None
        Retain columns after parsing.
        If validation variable defined reqruied columns, this should be None; validation error.
    chains: Optional[List] = Field(description="chain_for_data_processing.", default=None)
        Chain process for data manipulation.
        Refer to each estimator/transformer class in the chain for required variables.
    validation: Optional[Dict] = Field(description="data_validation.", default=None)
        Variables for DataFrame validation.
        Refer to validation class.
    ----------
    """

    wfclss: ClassVar[str] = "DataIngestion"

    format: Literal["df", "csv", "parquet"]
    target_db: Optional[str] = None
    target_output: Optional[str] = None
    compression: Optional[str] = None
    partition_cols: Optional[list] = None
    parser: Dict = Field(description="parser.")
    retain_cols: Optional[List] = None
    chains: Optional[List] = Field(
        description="chain_for_data_processing.", default=None
    )
    validation: Optional[Dict] = Field(description="data_validation.", default=None)

    @property
    def variables(self) -> Dict:
        "Return variables"
        return super().variables

    def run(self):
        """Execute workflow"""
        v = self.variables

        # Set up a logger.
        logger = self.logger
        logger.info("Start Data Ingestion Workflow")

        # Load a parser class.
        logger.info(f"Parsing raw data by '{v["parser"]["kind"]}'")  # add cls name
        cls = load_class(v["parser"])
        df = cls.parse()

        # Chain process for data manipluation.
        if v["chains"]:
            logger.info("Enter chain process for data manipulation")
            chain = ChainProcess.create_chain(v["chains"])
            _, df = chain.transform(df)

        # Retain required columns.
        if v["retain_cols"]:
            logger.info("Retain required columns")
            df = df[v["retain_cols"]]

        # Validate DataFrame.
        if v["validation"]:
            logger.info("Validate DataFrame")
            df_validator = Validator.model_validate(v["validation"])
            df = df_validator.valid(df)
            logger.info("Validated DataFrame")

        # Store data with dedicated format.
        if v["format"] == "df":
            logger.info(f"Output format is '{v["format"]}'. Return DataFrame")
            return df
        else:
            raise NotImplementedError(f"Unsupported format: {v["format"]}")
