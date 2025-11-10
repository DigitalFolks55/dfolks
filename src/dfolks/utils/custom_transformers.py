"""Transformers for data ingestion.

Need to work:
0) Add more transformers
1) Update ReplaceNanStrTransformer enable dictionary input
2) Documentation
"""

import logging
from typing import ClassVar, Dict

import pandas as pd

from dfolks.core.classfactory import NormalClassRegistery

# Set up shared logger
logger = logging.getLogger("shared")


class RemoveNanColsTransformer(NormalClassRegistery):
    """Remove columns with NaN values above a threshold.

    Variables
    ----------
    threshold: define a threshold to remove columns.
        float = 0.5
        i.e. if 50% of values are NaN, remove the column.
    """

    nmclss: ClassVar[str] = "RemoveNanColsTransformer"

    threshold: float = 0.5

    @property
    def variables(self) -> Dict:
        return super().variables

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        v = self.variables
        n_row = len(df)

        # Remove columns with NaN values above a threshold
        df = df.loc[:, (df.isnull().sum(axis=0) / n_row < v["threshold"])]

        return df
