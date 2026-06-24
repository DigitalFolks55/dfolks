"""XBRL parsers.

Need to do
0) XBRL input: not valiable but def?
1) More add functionality and values
"""

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
    model_xbrl: XBRL model.
        Any
    source_path: Source of ingestion, e.g. file name.
        str
    ----------
    """

    nmclss: ClassVar[str] = "EdinetXbrlParser"
    model_xbrl: Any
    source_path: str

    @property
    def variables(self) -> Dict:
        return super().variables

    @property
    def _tag_lists(self) -> Dict:
        tag_dict = {
            "Japan GAAP": {
                "NetSalesSummaryOfBusinessResults": "Net sales",  # 売上高
                "OrdinaryIncomeSummaryOfBusinessResults": "Net sales",  # 売上高
                "OperatingRevenue1SummaryOfBusinessResults": "Net sales",  # 売上高
                "OperatingRevenue2SummaryOfBusinessResults": "Net sales",  # 売上高
                "RevenueKeyFinancialData": "Net sales",  # 売上高
                "RevenueSummaryOfBusinessResults": "Net sales",  # 売上高
                "OperatingIncomeINS": "Net sales",  # 売上高
                "OperatingIncome": "Operating profit",  # 営業利益
                "OperatingRevenue1": "Operating profit",  # 営業利益
                "IncomeBeforeIncomeTaxes": "Earnings before interest and taxes",  # 税引前利益
                "ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults": "Profit",  # 当期純利益
                "NetIncomeLossSummaryOfBusinessResults": "Profit",  # 当期純利益
                "ComprehensiveIncomeSummaryOfBusinessResults": "Comprehensive profit",  # 包括利益
                "NetCashProvidedByUsedInOperatingActivitiesSummaryOfBusinessResults": "Operating cashflow",
                # 営業活動によるキャッシュフロー
                "NetCashProvidedByUsedInInvestingActivitiesSummaryOfBusinessResults": "Investing cashflow",
                # 投資活動によるキャッシュフロー
                "NetCashProvidedByUsedInFinancingActivitiesSummaryOfBusinessResults": "Financing cashflow",
                # 財務活動によるキャッシュフロー
                "CashAndCashEquivalentsSummaryOfBusinessResults": "Cash equilavents",  # 現金及び現金同等物
                "NetAssetsSummaryOfBusinessResults": "Equity",  # 純資産
                "TotalAssetsSummaryOfBusinessResults": "Total assests",  # 総資産
                "EquityToAssetRatioSummaryOfBusinessResults": "Equity to assets",  # 自己資本比率
                "NetAssetsPerShareSummaryOfBusinessResults": "Book value per share",  # 一株当たり純資産, BPS
                "BasicEarningsLossPerShareSummaryOfBusinessResults": "Earning per share",  # 一株当たり当期純利益, EPS
                "PriceEarningsRatioSummaryOfBusinessResults": "Price earnings ratios",  # 株価収益率, PER
                "NumberOfEmployees": "Employees",  # 従業員
            },
            "IFRS": {
                "RevenueIFRSSummaryOfBusinessResults": "Net sales",  # 売上高
                "SalesAndFinancialServicesRevenueIFRSKeyFinancialData": "Net sales",  # 売上高
                "OperatingRevenuesIFRSKeyFinancialData": "Net sales",  # 売上高
                "NetSalesIFRSKeyFinancialData": "Net sales",  # 売上高
                "NetSalesIFRSSummaryOfBusinessResults": "Net sales",  # 売上高
                "InsuranceRevenueIFRSKeyFinancialData": "Net sales",  # 売上高
                "OperatingProfitLossIFRSKeyFinancialData": "Operating profit",  # 営業利益
                "OperatingProfitLossIFRS": "Operating profit",  # 営業利益
                "ProfitLossBeforeTaxIFRSSummaryOfBusinessResults": "Earnings before interest and taxes",  # 税引前利益
                "ProfitLossBeforeTaxIFRS": "Earnings before interest and taxes",  # 税引前利益
                "ProfitBeforeFinancingAndIncomeTaxIFRSKeyFinancialData": "Earnings before interest and taxes",  # 税引前利益
                "ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults": "Profit",  # 当期純利益
                "ComprehensiveIncomeAttributableToOwnersOfParentIFRSSummaryOfBusinessResults": "Comprehensive profit",
                # 包括利益
                "ComprehensiveIncomeIFRSSummaryOfBusinessResults": "Comprehensive profit",  # 包括利益
                "CashFlowsFromUsedInOperatingActivitiesIFRSSummaryOfBusinessResults": "Operating cashflow",
                # 営業活動によるキャッシュフロー
                "CashFlowsFromUsedInInvestingActivitiesIFRSSummaryOfBusinessResults": "Investing cashflow",
                # 投資活動によるキャッシュフロー
                "CashFlowsFromUsedInFinancingActivitiesIFRSSummaryOfBusinessResults": "Financing cashflow",
                # 財務活動によるキャッシュフロー
                "CashAndCashEquivalentsIFRSSummaryOfBusinessResults": "Cash equilavents",  # 現金及び現金同等物
                "EquityIFRS": "Equity",  # 純資産
                "TotalAssetsIFRSSummaryOfBusinessResults": "Total assests",  # 総資産
                "RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults": "Equity to assets",  # 自己資本比率
                "EquityToAssetRatioIFRSSummaryOfBusinessResults": "Book value per share",  # 一株当たり純資産, BPS
                "BasicEarningsLossPerShareIFRSSummaryOfBusinessResults": "Earning per share",  # 一株当たり当期純利益, EPS
                "PriceEarningsRatioIFRSSummaryOfBusinessResults": "Price earnings ratios",  # 株価収益率, PER
                "NumberOfEmployees": "Employees",  # 従業員
            },
            "US GAAP": {
                "RevenuesUSGAAPSummaryOfBusinessResults": "Net sales",  # 売上高
                "OperatingIncomeLossUSGAAPSummaryOfBusinessResults": "Operating profit",  # 営業利益
                "ProfitLossBeforeTaxUSGAAPSummaryOfBusinessResults": "Earnings before interest and taxes",  # 税引前利益
                "NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults": "Profit",  # 当期純利益
                "ComprehensiveIncomeUSGAAPSummaryOfBusinessResults": "Comprehensive profit",  # 包括利益
                "ComprehensiveIncomeAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults": "Comprehensive profit",
                # 包括利益
                "CashFlowsFromUsedInOperatingActivitiesUSGAAPSummaryOfBusinessResults": "Operating cashflow",
                # 営業活動によるキャッシュフロー
                "CashFlowsFromUsedInInvestingActivitiesUSGAAPSummaryOfBusinessResults": "Investing cashflow",
                # 投資活動によるキャッシュフロー
                "CashFlowsFromUsedInFinancingActivitiesUSGAAPSummaryOfBusinessResults": "Financing cashflow",
                # 財務活動によるキャッシュフロー
                "CashAndCashEquivalentsUSGAAPSummaryOfBusinessResults": "Cash equilavents",  # 現金及び現金同等物
                "EquityIncludingPortionAttributableToNonControllingInterestUSGAAPSummaryOfBusinessResults": "Equity",
                # 純資産
                "NetAssetsSummaryOfBusinessResults": "Equity",  # 純資産
                "TotalAssetsUSGAAPSummaryOfBusinessResults": "Total assests",  # 総資産
                "EquityToAssetRatioUSGAAPSummaryOfBusinessResults": "Equity to assets",  # 自己資本比率
                "EquityAttributableToOwnersOfParentPerShareUSGAAPSummaryOfBusinessResults": "Book value per share",
                # 一株当たり純資産, BPS
                "BasicEarningsLossPerShareUSGAAPSummaryOfBusinessResults": "Earning per share",  # 一株当たり当期純利益, EPS
                "PriceEarningsRatioUSGAAPSummaryOfBusinessResults": "Price earnings ratios",  # 株価収益率, PER
                "NumberOfEmployees": "Employees",  # 従業員
            },
        }

        return tag_dict

    def load(self):
        return self.model_xbrl

    def parse(self):
        tag_dict = self._tag_lists
        rows = []
        df = []

        logger.info("Map common metadata from XBRL files.")
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

        logger.info("Map financial data from XBRL files.")
        for fct in self.model_xbrl.facts:
            tag_dicts = tag_dict.get(acc_standard, {})
            if len(tag_dicts) == 0:
                logger.error(f"No tags defined for accounting standard: {acc_standard}")
                continue
            else:
                lists = tag_dicts.keys()
            if fct.concept.qname.localName in lists:
                if (
                    fct.contextID.endswith("CurrentYearDuration")
                    or fct.contextID.endswith(
                        "CurrentYearDuration_NonConsolidatedMember"
                    )
                    or fct.contextID.endswith("CurrentYearInstant")
                    or fct.contextID.endswith(
                        "CurrentYearInstant_NonConsolidatedMember"
                    )
                ):
                    rows.append(
                        {
                            "Ticker": stock_code,
                            "Edinet code": edinet_code,
                            "attribute": tag_dicts.get(fct.concept.qname.localName),
                            "label": fct.concept.label(),
                            "context": fct.contextID,
                            "value": fct.value,
                            "Period start date": fy_start_date,
                            "Period end date": fy_end_date,
                            "unit": fct.unitID,
                            "Account standard": acc_standard,
                        }
                    )

        logger.info(
            f"Total {len(rows)} financial data points extracted from XBRL file."
        )
        df.append(pd.DataFrame(rows))
        logger.info("Combine extracted data into one DataFrame.")
        df = pd.concat(df, ignore_index=True).drop_duplicates()

        if df.empty:
            logger.warning("No data extracted from XBRL file.")
            return pd.DataFrame()  # Return empty DataFrame if no data was extracted

        logger.info("Pivoting DataFrame to have attributes as columns.")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df_pivot = df.pivot_table(
            index=[
                "Ticker",
                "Edinet code",
                "Period start date",
                "Period end date",
                "Account standard",
            ],
            columns="attribute",
            values="value",
        ).reset_index()

        df_pivot["source_path"] = self.source_path

        logger.info("Finished parsing XBRL file and pivoting DataFrame.")

        return df_pivot
