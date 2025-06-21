"""classfactory base classes.
1) TransformerRegistery: base class for customized transformer.
2) WorkflowsRegistry: base class for workflow.

Key libraries:
pydantic: parsing and validating variables.
ClassRegistry: managing class - register to avoid collision.
ABC: absract base class for defining abstract methods.
BaseEstimator and TransformerMixin: scikit-learn base classes for transformers.

Need to work:
0) more attributes for base classes
1) documentation
2) update
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

from class_registry import ClassRegistry
from class_registry.base import AutoRegister
from pydantic import BaseModel
from sklearn.base import BaseEstimator, TransformerMixin

from dfolks.core.utils import import_all_submodules, set_logger

# Class registry, all sub-classes will be stored at attr_name and not allow duplication.
__reg_transformer_cls__ = ClassRegistry(attr_name="trsclss", unique=True)
__reg_workflow_cls__ = ClassRegistry(attr_name="wflcls", unique=True)
__reg_parser_cls__ = ClassRegistry(attr_name="prcls", unique=True)


def allow_overwrite_classes():
    """Allow overwriting in the class registry"""
    global __reg_transformer_cls__, __reg_workflow_cls__
    __reg_transformer_cls__.unique = False
    __reg_workflow_cls__.unique = False
    __reg_parser_cls__.unique = False


class TransformerRegistery(
    AutoRegister(__reg_transformer_cls__),
    ABC,
    BaseEstimator,
    TransformerMixin,
    BaseModel,
):
    """Base class for customized transformer.

    Inherited class must have following values
    1) trsclss: class registration.

    Key methods
    ----------
    variables: Return variables of the workflow.
    ----------

    Key methods
    ----------
    fit: Abstract method.
        Performing fit.
    transform: Abstract method.
        Performing transformation.
    ----------
    """

    @property
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()

    @abstractmethod
    def fit(self, X, y=None):
        """fit method"""
        raise NotImplementedError("fit method not implemented")

    @abstractmethod
    def transform(self, X):
        """transform method"""
        raise NotImplementedError("transform method not implemented")


class WorkflowsRegistry(AutoRegister(__reg_workflow_cls__), ABC, BaseModel):
    """Workflow base class.

    Inherited class must have following values
    1) wflcls: class registration.

    Key methods
    ----------
    run: Abstract method.
        Execute overall workflow. To be implemented at subclasses.
    logger: set up a logger for workflow.
    variables: Return variables of the workflow.
    ----------

    Variables
    ----------
    log_level: define level of log.
        Optional[str] = "INFO"
    log_path: Set a path if you want to store a log in a file.
        Optional[str] = None
    """

    log_level: Optional[str] = "INFO"
    log_path: Optional[str] = None

    @abstractmethod
    def run(self) -> None:
        """Run the workflow."""
        raise NotImplementedError("run method not implemented")

    @property
    def logger(self) -> logging.Logger:
        """Set up a logger of workflow."""
        v = self.variables
        name = "shared"
        level = getattr(logging, v["log_level"])
        log = set_logger(name, level, v["log_path"])
        return log

    @property
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()


class AbstractParser(ABC, BaseModel):
    """Abstract parser for data ingestion.

    Key methods
    ----------
    load: Abstract method to load data from a file or files in a directory
        This should be implemented at subclasses.
    parse: Abstract method to parse an ingested data.
        This should be implemented at subclasses.
    ----------
    """

    @abstractmethod
    def load():
        "Load method. This should be implemented at subclasses."
        raise NotImplementedError()

    @abstractmethod
    def parse():
        "Parse method. This should be implemented at subclasses."
        raise NotImplementedError()

    @property
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()


def check_registration():
    """Imports all submodules and checks the registered classes."""
    # Import all modules in the specified package.
    import_all_submodules()

    # Print out all registered classes.
    registered_classes_transformer = __reg_transformer_cls__._registry
    print("Registered Transformer Classes:")
    for name, cls in registered_classes_transformer.items():
        print(f"{name}: {cls}")

    registered_classes_workflows = __reg_workflow_cls__._registry
    print("Registered Workflow Classes:")
    for name, cls in registered_classes_workflows.items():
        print(f"{name}: {cls}")

    return registered_classes_transformer, registered_classes_workflows
