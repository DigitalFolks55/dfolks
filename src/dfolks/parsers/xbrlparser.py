"""XBRL parsers"""

import logging
from typing import Any, ClassVar, Dict

import pandas as pd

from dfolks.core.classfactory import NormalClassRegistery

# Set up shared logger
logger = logging.getLogger("shared")


class EdinetXbrlParser(NormalClassRegistery):
    """EDINET XBRL parser.

    Variables
    ----------
    source_path: str
        Source path.
    ----------
    """

    nmclss: ClassVar[str] = "EdinetXbrlParser"
    model_xbrl: Any

    @property
    def variables(self) -> Dict:
        return super().variables

    @property
    def _tag_lists(self) -> Dict:
        tag_dict = {
            "Japan GAAP": {
                "NetSalesSummaryOfBusinessResults": "net_sales",
                "OrdinaryIncomeSummaryOfBusinessResults": "net_sales",
                "OperatingIncome": "operating_profit",
                "OperatingRevenue1": "operating_profit",
                "IncomeBeforeIncomeTaxes": "earnings_before_interest_taxes",
                "ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults": "profit",
                "ComprehensiveIncomeSummaryOfBusinessResults": "comprehensive_profit",
                "NetCashProvidedByUsedInOperatingActivitiesSummaryOfBusinessResults": "operating_cash_flow",
                "NetCashProvidedByUsedInInvestingActivitiesSummaryOfBusinessResults": "investment_cash_flow",
                "NetCashProvidedByUsedInFinancingActivitiesSummaryOfBusinessResults": "financing_cash_flow",
                "CashAndCashEquivalentsSummaryOfBusinessResults": "cash_equivalents",
                "NetAssetsSummaryOfBusinessResults": "equity",
                "TotalAssetsSummaryOfBusinessResults": "total_assets",
                "EquityToAssetRatioSummaryOfBusinessResults": "equity_to_assets",
                "NetAssetsPerShareSummaryOfBusinessResults": "book_value_per_share",
                "BasicEarningsLossPerShareSummaryOfBusinessResults": "earnings_per_share",
                "PriceEarningsRatioSummaryOfBusinessResults": "price_earnings_ratios",
                "NumberOfEmployees": "employees",
            },
            "IFRS": {
                "RevenueIFRSSummaryOfBusinessResults": "net_sales",
                "SalesAndFinancialServicesRevenueIFRSKeyFinancialData": "net_sales",
                "OperatingRevenuesIFRSKeyFinancialData": "net_sales",
                "OperatingProfitLossIFRSKeyFinancialData": "operating_profit",
                "OperatingProfitLossIFRS": "operating_profit",
                "ProfitLossBeforeTaxIFRSSummaryOfBusinessResults": "earnings_before_interest_taxes",
                "ProfitLossBeforeTaxIFRS": "earnings_before_interest_taxes",
                "ProfitBeforeFinancingAndIncomeTaxIFRSKeyFinancialData": "earnings_before_interest_taxes",
                "ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults": "profit",
                "ComprehensiveIncomeAttributableToOwnersOfParentIFRSSummaryOfBusinessResults": "comprehensive_profit",
                "ComprehensiveIncomeIFRSSummaryOfBusinessResults": "comprehensive_profit",
                "CashFlowsFromUsedInOperatingActivitiesIFRSSummaryOfBusinessResults": "operating_cash_flow",
                "CashFlowsFromUsedInInvestingActivitiesIFRSSummaryOfBusinessResults": "investment_cash_flow",
                "CashFlowsFromUsedInFinancingActivitiesIFRSSummaryOfBusinessResults": "financing_cash_flow",
                "CashAndCashEquivalentsIFRSSummaryOfBusinessResults": "cash_equivalents",
                "EquityIFRS": "equity",
                "TotalAssetsIFRSSummaryOfBusinessResults": "total_assets",
                "RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults": "equity_to_assets",
                "EquityToAssetRatioIFRSSummaryOfBusinessResults": "book_value_per_share",
                "BasicEarningsLossPerShareIFRSSummaryOfBusinessResults": "earnings_per_share",
                "PriceEarningsRatioIFRSSummaryOfBusinessResults": "price_earnings_ratios",
                "NumberOfEmployees": "employees",
            },
        }

        return tag_dict

    def load(self):
        return self.model_xbrl

    def parse(self):
        tag_dict = self._tag_lists
        rows = []
        df = []

        for fct in self.model_xbrl.facts:
            if fct.concept.qname.localName == "SecurityCodeDEI":
                stock_code = fct.value

            if fct.concept.qname.localName == "EDINETCodeDEI":
                edinet_code = fct.value

            if fct.concept.qname.localName == "AccountingStandardsDEI":
                acc_standard = fct.value

            if fct.concept.qname.localName == "CurrentFiscalYearStartDateDEI":
                fy_start_date = fct.value

            if fct.concept.qname.localName == "CurrentFiscalYearEndDateDEI":
                fy_end_date = fct.value

        for fct in self.model_xbrl.facts:
            tag_dicts = tag_dict.get(acc_standard, {})
            if len(tag_dicts) == 0:
                logging.error(
                    f"No tags defined for accounting standard: {acc_standard}"
                )
                continue
            else:
                lists = tag_dicts.keys()
            if fct.concept.qname.localName in lists:
                if fct.contextID.endswith(
                    "CurrentYearDuration"
                ) or fct.contextID.endswith("CurrentYearInstant"):
                    rows.append(
                        {
                            "stock_code": stock_code,
                            "edinet_code": edinet_code,
                            "attribute": tag_dicts.get(fct.concept.qname.localName),
                            "label": fct.concept.label(),
                            "context": fct.contextID,
                            "value": fct.value,
                            "period_start_date": fy_start_date,
                            "period_end_date": fy_end_date,
                            "unit": fct.unitID,
                            "account_standard": acc_standard,
                        }
                    )

        df.append(pd.DataFrame(rows))

        df = pd.concat(df, ignore_index=True).drop_duplicates()
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df_pivot = df.pivot_table(
            index=[
                "stock_code",
                "edinet_code",
                "period_start_date",
                "period_end_date",
                "account_standard",
            ],
            columns="attribute",
            values="value",
        ).reset_index()

        return df_pivot
