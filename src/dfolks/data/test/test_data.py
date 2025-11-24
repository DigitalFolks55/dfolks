"""Test for data.

Need to do:
0) Add tests of input.py with different file types.
1) Add tests of output.py for parquet when we enable the function of saving to parquet.
"""

import unittest
from typing import ClassVar

import pandas as pd
import yaml

from dfolks.core.classfactory import NormalClassRegistery
from dfolks.data.data import (
    Validator,
)
from dfolks.data.input import (
    load_flat_file,
)


class TstParser(NormalClassRegistery):
    """Test parser class."""

    nmclss: ClassVar[str] = "TestParser"

    path: str = None
    load_all: bool = None

    @property
    def variables(self):
        """Return Variables of a pydantic model."""
        return self.model_dump()

    def load(self):
        """Load method."""
        v = self.variables
        df = load_flat_file(path=v["path"], load_all=v["load_all"])
        return df

    def parse(self):
        """Parse method."""
        parsed_df = self.load()
        return parsed_df


class TestParserCls(unittest.TestCase):
    """Test abstract parser class."""

    def setUp(self):
        self.expected_output = pd.DataFrame(
            {"Column1": ["A", "B", "C"], "Column2": [1, 2, 3]}
        )

        self.expected_parsed_output = pd.DataFrame(
            {"Column1": ["A", "B", "C"], "Column2": ["1", "2", "3"]}
        )

        self.schema_yaml = """
        schemas:
            "Column1":
                type: String
                nullable: false
                unique: true
            "Column2":
                type: String
                nullable: false
                unique: true
        """
        self.schema_dict = yaml.safe_load(self.schema_yaml)

        self.test_parser_csv = TstParser.model_validate(
            {"path": "src/dfolks/data/test/dummy/dummy.csv", "load_all": False}
        )

        self.test_parser_xlsx = TstParser.model_validate(
            {"path": "src/dfolks/data/test/dummy/dummy.xlsx", "load_all": False}
        )

        self.test_validator = Validator.model_validate(self.schema_dict)

    def test_is_instance(self):
        self.assertIsInstance(self.test_parser_csv, NormalClassRegistery)
        self.assertIsInstance(self.test_parser_xlsx, NormalClassRegistery)
        self.assertIsInstance(self.test_validator, Validator)

    def test_load_method(self):
        pd.testing.assert_frame_equal(self.test_parser_csv.load(), self.expected_output)
        pd.testing.assert_frame_equal(
            self.test_parser_xlsx.load(), self.expected_output
        )

    def test_parse_method(self):
        pd.testing.assert_frame_equal(
            self.test_parser_csv.parse(), self.expected_output
        )
        pd.testing.assert_frame_equal(
            self.test_parser_xlsx.parse(), self.expected_output
        )

    def test_validation(self):
        validated_df_csv = self.test_validator.valid(self.test_parser_csv.parse())
        validated_df_xlsx = self.test_validator.valid(self.test_parser_xlsx.parse())
        pd.testing.assert_frame_equal(validated_df_csv, self.expected_parsed_output)
        pd.testing.assert_frame_equal(validated_df_xlsx, self.expected_parsed_output)


def test_load_files_from_dir():
    path = "src/dfolks/data/test/dummy/"
    df = load_flat_file(path=path, load_all=True)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty


if __name__ == "__main__":
    unittest.main()
