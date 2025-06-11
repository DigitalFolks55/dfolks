"""Test for variablized class."""

from typing import ClassVar

from dfolks.core.classfactory import (
    TransformerRegistery,
    WorkflowsRegistry,
    allow_overwrite_classes,
    check_registration,
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
    wflcls: ClassVar[str] = "testcls1"
    test_var: int

    def run(self):
        return self.wflcls


def test_workflow_registry():
    test_class = TstWfRegA.model_validate({"test_var": 100})
    assert test_class.wflcls == "testcls1"


def test_check_registration():
    rclss_trans, rclss_wfs = check_registration()

    assert len(rclss_trans) == 2
    assert len(rclss_wfs) == 1
