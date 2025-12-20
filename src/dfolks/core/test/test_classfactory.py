"""Test for classfactory."""

from typing import ClassVar

import pytest
from pydantic import BaseModel

from dfolks.core.classfactory import (
    NormalClassRegistery,
    TransformerRegistery,
    WorkflowsRegistry,
    allow_overwrite_classes,
    load_class,
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


class FakeRegistry:
    def __init__(self, mapping=None):
        self._registry = dict(mapping or {})

    def __contains__(self, key):
        return key in self._registry


class DummyTransformer(BaseModel):
    kind: str
    a: int = 1


class DummyWorkflow(BaseModel):
    kind: str
    name: str = "wf"


class DummyNormal(BaseModel):
    kind: str
    x: float = 0.0


@pytest.fixture
def patch_registries(monkeypatch):
    transformer = FakeRegistry({"dummy_transformer": DummyTransformer})
    workflow = FakeRegistry({"dummy_workflow": DummyWorkflow})
    normal = FakeRegistry({"dummy_normal": DummyNormal})

    monkeypatch.setattr("dfolks.core.classfactory.__reg_transformer_cls__", transformer)
    monkeypatch.setattr("dfolks.core.classfactory.__reg_workflow_cls__", workflow)
    monkeypatch.setattr("dfolks.core.classfactory.__reg_normal_cls__", normal)

    return transformer, workflow, normal


def test_load_class_from_dict_transformer(patch_registries):
    obj = load_class({"kind": "dummy_transformer", "a": 10})
    assert isinstance(obj, DummyTransformer)
    assert obj.kind == "dummy_transformer"
    assert obj.a == 10


def test_load_class_from_dict_workflow(patch_registries):
    obj = load_class({"kind": "dummy_workflow", "name": "abc"})
    assert isinstance(obj, DummyWorkflow)
    assert obj.name == "abc"


def test_load_class_from_dict_normal(patch_registries):
    obj = load_class({"kind": "dummy_normal", "x": 1.25})
    assert isinstance(obj, DummyNormal)
    assert obj.x == 1.25


def test_load_class_from_yaml_string(patch_registries):
    yml = """
        kind: dummy_transformer
        a: 7
    """
    obj = load_class(yml)
    assert isinstance(obj, DummyTransformer)
    assert obj.a == 7


def test_load_class_raises_if_kind_missing(patch_registries):
    with pytest.raises(ValueError) as e:
        load_class({"a": 1})
    assert "kind" in str(e.value)


def test_load_class_raises_if_not_registered(patch_registries):
    with pytest.raises(ValueError) as e:
        load_class({"kind": "UnknownKind"})
    assert "not registered" in str(e.value)
