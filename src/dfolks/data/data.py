"""Data related classes.

1) NormalClassRegistery for Parse files.
2) Validator for validating df; duplication, null etc.

Need to work:
0) Pydantic validation of variables at Validator (Schemas).
0-1) Error handling.
1) Enable set up pks, partitions at Valdator.
2) GX (Later due to compability issue).
3) Compatible with hadoop/spark using hdfs (Future work with pyspark).
"""

import logging
from abc import ABC
from typing import Dict

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
        1) type: data type
        2) nullable: default True  # Allow null or not
        3) unique: default False  # Allow duplication or not
    ----------
    """

    schemas: dict

    def valid(self, df):
        """Validate DataFrame."""
        v = self.variables
        logger.debug("Create Pandera DataFrame schema")
        columns = {}
        for col, dtype in v["schemas"].items():
            column = pa.Column(
                dtype["type"],
                nullable=dtype.get("nullable", True),
                unique=dtype.get("unique", False),
                coerce=True,
            )
            columns[col] = column

        # Define the DataFrame schema
        df_schema = pa.DataFrameSchema(columns, strict=True)

        # Drop not defined columns
        logger.info("Retain defined columns in the schema")
        df = df[df_schema.columns.keys()]

        # Return validated df
        logger.info("Enforce data type and check nullable and duplicated values")
        df_valid = df_schema.validate(df)
        return df_valid

    @property
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()
