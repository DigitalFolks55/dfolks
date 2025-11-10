"""Test for classfactory."""

from typing import ClassVar

from dfolks.core.classfactory import (
    NormalClassRegistery,
    TransformerRegistery,
    WorkflowsRegistry,
    allow_overwrite_classes,
)


class TstTrRegA(TransformerRegistery):
    trsclss: ClassVar[str] = "testcls1"
    test_var: int = 0

    def fit(self, X, y=None):
        return X

    def transform(self, X):
        return X


def test_transformer_reg_class():
    test_class = TstTrRegA.model_validate({"test_var": 100})
    assert test_class.trsclss == "testcls1"
    assert test_class.test_var == 100


def test_allow_overwrite_class():
    allow_overwrite_classes()

    class TstTrRegB(TransformerRegistery):
        trsclss: ClassVar[str] = "testcls2"

        def fit(self, X, y=None):
            return X

        def transform(self, X):
            return X

    class TstTrRegC(TransformerRegistery):
        trsclss: ClassVar[str] = "testcls1"

        def fit(self, X, y=None):
            return X

        def transform(self, X):
            return X


class TstWfRegA(WorkflowsRegistry):
    wfclss: ClassVar[str] = "testcls1"
    test_var: int

    def run(self):
        return self.wfclss


def test_workflow_registry():
    test_class = TstWfRegA.model_validate({"test_var": 100})
    assert test_class.wfclss == "testcls1"


class TstNormalRegA(NormalClassRegistery):
    nmclss: ClassVar[str] = "testnmcls1"
    test_var: int

    @property
    def variables(self):
        """Return Variables of a pydantic model."""
        return self.model_dump()


def test_normalcls_registry():
    test_class = TstNormalRegA.model_validate({"test_var": 100})
    assert test_class.nmclss == "testnmcls1"
    assert test_class.variables["test_var"] == 100
