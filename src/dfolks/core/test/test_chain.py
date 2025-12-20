"""Test for Chain Process."""

from typing import ClassVar, Dict

import pandas as pd

from dfolks.core.chain import ChainProcess
from dfolks.core.classfactory import (
    NormalClassRegistery,
    TransformerRegistery,
)


def test_chainprocess_children():
    """Test initialization of ChainProcess."""
    children = [
        {"kind": "test_transformer", "params": {}},
        {"kind": "test_normal", "params": {}},
    ]

    chain_process = ChainProcess(children=children)

    assert chain_process.children == children


def test_chainprocess_creat_chain():
    "Test create_chain method of ChainProcess." ""
    chains = [
        {"kind": "test_transformer", "params": {}},
        {"kind": "test_normal", "params": {}},
    ]

    chain_process = ChainProcess.create_chain(chains=chains)

    assert isinstance(chain_process, ChainProcess)
    assert chain_process.children == chains


def test_chainprocess_transform(monkeypatch):
    """Test transform method of ChainProcess."""
    # import dfolks.core.chain as chain_module

    class TestTransformer(TransformerRegistery):
        """Test transformer class."""

        trsclss: ClassVar[str] = "test_transformer"

        @property
        def variables(self) -> Dict:
            """Return Variables of a pydantic model."""
            return self.model_dump()

        def fit(self, X, y=None):
            return self

        def transform(self, X):
            return X

    class TestNormal(NormalClassRegistery):
        """Test normal class."""

        nmclss: ClassVar[str] = "test_normal"

        @property
        def variables(self) -> Dict:
            """Return Variables of a pydantic model."""
            return self.model_dump()

        def transform(self, X: pd.DataFrame) -> pd.DataFrame:
            return X

    def fake_load_class(child):
        kind = child["kind"]
        if kind == "test_transformer":
            return TestTransformer()
        elif kind == "test_normal":
            return TestNormal()
        else:
            raise ValueError(f"Unknown kind: {kind}")

    children = [
        {"kind": "test_transformer", "params": {}},
        {"kind": "test_normal", "params": {}},
    ]

    monkeypatch.setattr("dfolks.core.chain.load_class", fake_load_class)

    chain_process = ChainProcess(children=children)

    df_input = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    cls_dict, df_output = chain_process.transform(df=df_input)

    assert "test_transformer" in cls_dict
    assert "test_normal" in cls_dict
    pd.testing.assert_frame_equal(df_input, df_output)
