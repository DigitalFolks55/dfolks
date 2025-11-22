"""Chain process class.

Need to work:
1) documentation.
"""

from __future__ import annotations

import logging
from typing import Dict

from dfolks.core.classfactory import (
    NormalClassRegistery,
    TransformerRegistery,
    load_class,
)

# Set up a shared logger
logger = logging.getLogger("shared")


class ChainProcess:
    """Enable chain process of estimator and transformer.

    Key methods
    ----------
    transform: execute fit_transform for estimator or transform for transformer.
    create_chain: create a chain list of estimator and transformer.
    ----------
    """

    def __init__(self, children: list):
        """Initialize class.

        Class must have children as list.
        """
        self._children = children

    def transform(self, df):
        """Execute chain process."""
        cls_dict = {}

        for child in self.children:
            logger.info(f"Processing {child["kind"]} started")
            cls = load_class(child)
            if isinstance(cls, TransformerRegistery):
                fit_cls, df = cls.fit_transform(df)
                cls_dict.update({f"{child["kind"]}": fit_cls})
            elif isinstance(cls, NormalClassRegistery):
                df = cls.transform(df)
                cls_dict.update({f"{child["kind"]}": cls})
            else:
                raise NotImplementedError(
                    "TransformerRegistery or Normal class which has transform method is supported now."
                )
            logger.info(f"Processing {child["kind"]} completed")

        return cls_dict, df

    @property
    def children(self):
        """Return children variable."""
        return self._children

    @classmethod
    def create_chain(cls, chains):
        """Create a chain process as list."""
        chain_list = []
        for chain in chains:
            if issubclass(type(chain), Dict):
                chain_list.append(chain)
            else:
                raise NotImplementedError
        logger.info("Chain process successfully created")

        return cls(chain_list)
