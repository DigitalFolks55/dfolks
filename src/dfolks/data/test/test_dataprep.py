"""Test for data prep.

Need to do:
"""

import unittest

import pandas as pd

from dfolks.data.dataprep import DataExtractor


class TestDataPrepCls(unittest.TestCase):
    """Test data prep class."""

    def setUp(self):
        self.data_extractor = DataExtractor(
            base_df={
                "target_path": "src/dfolks/data/test/dummy/dummy1.csv",
                "schema": {
                    "Column1": {"type": "String", "nullable": False, "unique": False},
                    "Column3": {"type": "Int", "nullable": False, "unique": False},
                    "Column4": {"type": "String", "nullable": False, "unique": False},
                },
            },
            fillna_data=[
                {"column": "Column1", "value": "Unkown"},
                {"column": "Column3", "value": "max"},
                {"column": "Column4", "value": "CC"},
            ],
            filter_query="Column3 > 1",
            valid_final_df=True,
            schema_final_df={
                "kind": "DfValidator",
                "schemas": {
                    "Column1": {"type": "String", "nullable": False, "unique": False},
                    "Column3": {"type": "Int", "nullable": False, "unique": False},
                    "Column4": {"type": "String", "nullable": False, "unique": False},
                },
            },
            save_final_df=False,
        )

    def test_data_prep(self):
        """Test data prep method."""
        df_result = self.data_extractor.extract()
        expected_df = pd.DataFrame(
            {"Column1": ["B", "C"], "Column3": [5, 5], "Column4": ["BB", "CC"]}
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(df_result.reset_index(drop=True), expected_df)

    def test_merge_join_df(self):
        """Test merge join dataframes method."""
        self.data_extractor.join_dfs = [
            {
                "target_path": "src/dfolks/data/test/dummy/dummy.csv",
                "join_type": "left",
                "join_keys": ["Column1"],
                "schemas": {
                    "Column1": {"type": "String", "nullable": False, "unique": False},
                    "Column2": {"type": "Int", "nullable": False, "unique": False},
                },
            }
        ]
        self.data_extractor.schema_final_df = {
            "kind": "DfValidator",
            "schemas": {
                "Column1": {"type": "String", "nullable": False, "unique": False},
                "Column2": {"type": "Int", "nullable": False, "unique": False},
                "Column3": {"type": "Int", "nullable": False, "unique": False},
            },
        }
        df_result = self.data_extractor.extract()
        expected_df = pd.DataFrame(
            {"Column1": ["B", "C"], "Column2": [2, 3], "Column3": [5, 5]}
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(df_result.reset_index(drop=True), expected_df)


if __name__ == "__main__":
    unittest.main()
