"""Chain process class.

Need to work:
1) documentation.
"""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from dfolks.core.classfactory import (
    NormalClassRegistery,
    TransformerRegistery,
    load_class,
)

# Set up a shared logger
logger = logging.getLogger("shared")


class ChainProcess:
    """Enable chain process of estimator and transformer.
    All children classes are stored as a dict and are passed as output after processing.

    Key methods
    ----------
    transform:
        Execute fit_transform for estimator (TransformerRegistery) or transform for transformer (NormalClassRegistry).
    create_chain: Create a chain list of estimator and transformer.
    children: Return children classes as a list.
    ----------
    """

    def __init__(self, children: list):
        """Initialize class.

        Class must have any children classes as a list.
        """
        self._children = children

    def transform(self, df: pd.DataFrame) -> Dict | pd.DataFrame:
        """Execute chain process."""
        cls_dict = {}

        for child in self.children:
            logger.info(f"Processing {child['kind']} started")
            cls = load_class(child)
            # If cls is TransformerRegistery, conduct fit_transform
            if isinstance(cls, TransformerRegistery):
                cls.fit(df)
                df = cls.transform(df)
                cls_dict.update({f"{child["kind"]}": cls})
            # If cls is NormalClassRegistery, conduct transform
            elif isinstance(cls, NormalClassRegistery):
                df = cls.transform(df)
                cls_dict.update({f"{child["kind"]}": cls})
            else:
                raise NotImplementedError(
                    "TransformerRegistery or Normal class which has transform method is supported now."
                )
            logger.info(f"Processing {child['kind']} completed")

        return cls_dict, df

    @property
    def children(self) -> Dict:
        """Return children variable."""
        return self._children

    @classmethod
    def create_chain(cls, chains) -> ChainProcess:
        """Create a chain process as a list."""
        chain_list = []
        for chain in chains:
            if issubclass(type(chain), Dict):
                chain_list.append(chain)
            else:
                raise NotImplementedError
        logger.info("Chain process successfully created")

        return cls(chain_list)
