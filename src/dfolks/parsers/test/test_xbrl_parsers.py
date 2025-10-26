"""Test for data.

Need to do:
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from dfolks.parsers.xbrlparser import EdinetXbrlParser


class MockQName:
    def __init__(self, localName):
        self.localName = localName


class MockConcept:
    def __init__(self, localName, label_text):
        self.qname = MockQName(localName)
        self._label = label_text

    def label(self):
        return self._label


class MockFact:
    def __init__(
        self, localName, value, contextID="ContextRef_CurrentYearDuration", unitID="JPY"
    ):
        self.concept = MockConcept(localName, f"Label for {localName}")
        self.value = value
        self.contextID = contextID
        self.unitID = unitID


@pytest.fixture
def mock_model_xbrl():
    facts = [
        # Metadata facts
        MockFact("SecurityCodeDEI", "7203"),
        MockFact("EDINETCodeDEI", "E00001"),
        MockFact("AccountingStandardsDEI", "Japan GAAP"),
        MockFact("CurrentFiscalYearStartDateDEI", "2024-04-01"),
        MockFact("CurrentFiscalYearEndDateDEI", "2025-03-31"),
        # Financial facts
        MockFact(
            "NetSalesSummaryOfBusinessResults", "100000", "ctx_CurrentYearDuration"
        ),
        MockFact("OperatingIncome", "20000", "ctx_CurrentYearDuration"),
        MockFact(
            "ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults",
            "15000",
            "ctx_CurrentYearInstant",
        ),
    ]
    return SimpleNamespace(facts=facts)


def test_parse_edinet_xbrl(mock_model_xbrl):
    parser = EdinetXbrlParser(model_xbrl=mock_model_xbrl)
    df = parser.parse()

    # --- Assertions ---
    assert not df.empty
    assert "net_sales" in df.columns
    assert "operating_profit" in df.columns
    assert "profit" in df.columns

    # Check metadata columns
    assert df["stock_code"].iloc[0] == "7203"
    assert df["edinet_code"].iloc[0] == "E00001"
    assert df["account_standard"].iloc[0] == "Japan GAAP"

    # Check numeric conversion
    assert pd.api.types.is_numeric_dtype(df["net_sales"])
    assert df["net_sales"].iloc[0] == 100000
