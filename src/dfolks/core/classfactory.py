"""classfactory base classes.
1) TransformerRegistery: base class for customized transformer.
2) WorkflowsRegistry: base class for workflow.
3) NormalClassRegistery: base class for normal classes.

Key libraries:
pydantic: parsing and validating variables.
ClassRegistry: managing class - register to avoid collision.
ABC: absract base class for defining abstract methods.
BaseEstimator and TransformerMixin: scikit-learn base classes for transformers.

Need to work:
0) more attributes for base classes
1) documentation
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

import yaml
from class_registry import ClassRegistry
from class_registry.base import AutoRegister
from pydantic import BaseModel
from sklearn.base import BaseEstimator, TransformerMixin

from dfolks.core.modules import import_all_submodules, set_logger

# Class registry, all sub-classes will be stored at attr_name and not allow duplication.
__reg_transformer_cls__ = ClassRegistry(attr_name="trsclss", unique=True)
__reg_workflow_cls__ = ClassRegistry(attr_name="wfclss", unique=True)
__reg_normal_cls__ = ClassRegistry(attr_name="nmclss", unique=True)


def allow_overwrite_classes():
    """Allow overwriting in the class registry"""
    global __reg_transformer_cls__, __reg_workflow_cls__, __reg_normal_cls__
    __reg_transformer_cls__.unique = False
    __reg_workflow_cls__.unique = False
    __reg_normal_cls__.unique = False


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

    Abstract methods
    ----------
    fit: Performing fit.
    transform: Performing transformation.
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
    1) wfclss: class registration.

    Key methods
    ----------
    run: Abstract method.
        Execute overall workflow. To be implemented at subclasses.
    logger: Set up a logger for workflow.
    variables: Return variables of the workflow.
    ----------

    Variables
    ----------
    log_level: Define level of log.
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


class NormalClassRegistery(AutoRegister(__reg_normal_cls__), ABC, BaseModel):
    """Abstract parser for data ingestion.

    Inherited class must have following values
    1) nmclss: class registration.

    Key methods
    ----------
    variables: Abstract method.
        Return Variables of a pydantic model.
    ----------
    """

    @property
    @abstractmethod
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()


def check_registration():
    """Imports all submodules and checks the registered classes."""
    # Import all modules in the specified package.
    import_all_submodules()

    # Print out all registered classes.
    # transformer registry
    registered_classes_transformer = __reg_transformer_cls__._registry
    print("Registered Transformer Classes:")
    for name, cls in registered_classes_transformer.items():
        print(f"{name}: {cls}")

    # workflow registry
    registered_classes_workflows = __reg_workflow_cls__._registry
    print("Registered Workflow Classes:")
    for name, cls in registered_classes_workflows.items():
        print(f"{name}: {cls}")

    # normal class registry
    registered_normal_classes = __reg_normal_cls__._registry
    print("Registered Normal Classes:")
    for name, cls in registered_normal_classes.items():
        print(f"{name}: {cls}")

    # Return the registries for further use if needed.
    return (
        registered_classes_transformer,
        registered_classes_workflows,
        registered_normal_classes,
    )


def load_class(
    yml: Union[Dict[str, Any], str],
):
    """Load registered class from a yaml string or dict."""
    # Import all subclasses which are registered in entry points.
    import_all_submodules()

    # Parse YAML input; if it's already a dict, use it directly.
    if isinstance(yml, dict):
        vars = yml
    else:
        vars = yaml.safe_load(yml)

    # kind field is required to identify the class type.
    if "kind" not in vars:
        raise ValueError("The 'kind' field is required to load the class.")

    kind = vars["kind"]

    # Check in transformer registry
    if kind in __reg_transformer_cls__:
        cls = __reg_transformer_cls__._registry[kind]
        return cls.model_validate(vars)
    # Check in workflow registry
    elif kind in __reg_workflow_cls__:
        cls = __reg_workflow_cls__._registry[kind]
        return cls.model_validate(vars)
    # Check in normal class registry
    elif kind in __reg_normal_cls__:
        cls = __reg_normal_cls__._registry[kind]
        return cls.model_validate(vars)
    else:
        raise ValueError(f"Class with kind '{kind}' is not registered.")
