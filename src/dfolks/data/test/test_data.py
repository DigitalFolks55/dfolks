"""Test for data.

Need to do:
0) Add tests of input.py with different file types.
1) Add tests of output.py for parquet when we enable the function of saving to parquet.
"""

import tempfile
import unittest
from pathlib import Path
from typing import ClassVar

import pandas as pd
import yaml

from dfolks.core.classfactory import AbstractParser
from dfolks.data.data import (
    Validator,
)
from dfolks.data.input import (
    load_flat_file,
)
from dfolks.data.output import (
    save_to_flatfile,
)


class TstParser(AbstractParser):
    """Test parser class."""

    prcls: ClassVar[str] = "TestParser"
    path: str = None
    load_all: bool = None

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
                type: str
                nullable: false
                unique: true
            "Column2":
                type: str
                nullable: false
                unique: true
        """
        self.schema_dict = yaml.safe_load(self.schema_yaml)

        self.test_parser = TstParser.model_validate(
            {"path": "src/dfolks/data/test/dummy.csv", "load_all": False}
        )

        self.test_validator = Validator.model_validate(self.schema_dict)

    def test_is_instance(self):
        self.assertIsInstance(self.test_parser, AbstractParser)
        self.assertIsInstance(self.test_validator, Validator)

    def test_load_method(self):
        pd.testing.assert_frame_equal(self.test_parser.load(), self.expected_output)

    def test_parse_method(self):
        pd.testing.assert_frame_equal(self.test_parser.parse(), self.expected_output)

    def test_validation(self):
        validated_df = self.test_validator.valid(self.test_parser.parse())
        pd.testing.assert_frame_equal(validated_df, self.expected_parsed_output)


class TestSaveToFlatFile(unittest.TestCase):
    """Test"""

    def setUp(self):
        self.df = pd.DataFrame({"Column1": ["A", "B", "C"], "Column2": [1, 2, 3]})
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_save_to_flatfile_with_db(self):
        db = "test_db"
        table = "test_table"
        expected_file = self.temp_path / "FlatHive" / db / f"{table}.csv"

        with unittest.mock.patch("dfolks.data.output.__user_dic__", self.temp_path):
            save_to_flatfile(df=self.df, db=db, path=table, type="csv")

        self.assertTrue(expected_file.exists())

        # Check if the file was created
        loaded_df = pd.read_csv(expected_file)
        self.assertTrue(loaded_df.equals(self.df))

    def test_save_to_flatfile_without_db(self):
        out_file = self.temp_path / "output.csv"
        save_to_flatfile(self.df, db=None, path=str(out_file))

        self.assertTrue(out_file.exists())

        loaded_df = pd.read_csv(out_file)
        self.assertTrue(loaded_df.equals(self.df))


if __name__ == "__main__":
    unittest.main()
