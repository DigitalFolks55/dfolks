"""Data related classes.

1) Validator class for DataFrame validation.

Need to work:
0) More error handling.
1) GX (Later due to compability issue).
2) Compatible with hadoop/spark using hdfs (Future work with pyspark).
"""

import logging
import re
from abc import ABC
from typing import Dict

import pandas as pd
import pandera as pa
from pydantic import BaseModel

# Set up shared logger
logger = logging.getLogger("shared")


class Validator(ABC, BaseModel):
    """Data frame validator.

    Key methods
    ----------
    valid: validate df by Pandera.
        schemas should be properly defined.
    ----------

    Variables
    ----------
    schemas: dict
        DataFrame schemas.
        1) type: Data type
        2) nullable: Allow null or not  # Default True
        3) unique: Allow duplication or not  # Default False
        4) new_column: Rename column if needed. # Default False
        5) primary_key: Define primary key. # Default False
    ----------
    """

    schemas: Dict

    def valid(self, df) -> pd.DataFrame:
        """Validate DataFrame."""
        v = self.variables
        logger.debug("Create Pandera DataFrame schema")

        columns = {}
        rename_cols = {}

        for col, dtype in v["schemas"].items():
            # Need to add: check type in schema if no return error
            if re.search(r"Int|Float", dtype["type"]):
                logger.debug(f"Convert column {col} to numeric type")
                # Convert to numeric type to avoid validation error
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # Define column schema
            column = pa.Column(
                getattr(pa, dtype["type"]),
                nullable=dtype.get("nullable", True),
                unique=dtype.get("unique", False),
                required=True,
                coerce=True,
            )
            columns[col] = column

            # Update a dictionary for renaming columns
            if dtype.get("new_column", False):
                rename_cols[col] = dtype["new_column"]

        # Define the DataFrame schema
        df_schema = pa.DataFrameSchema(columns, strict=True)

        # Drop columns which are not defined in the schema
        logger.info("Retain defined columns in the schema")
        df = df[df_schema.columns.keys()]

        # Return validated df
        logger.info("Enforce data type and check nullable and duplicated values")
        df_valid = df_schema.validate(df)

        # Rename columns if needed
        if len(rename_cols) > 0:
            logger.info("Rename columns")
            df_valid = df_valid.rename(columns=rename_cols)

        return df_valid

    @property
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()
