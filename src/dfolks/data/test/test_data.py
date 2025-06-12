"""Test for data."""

import pandas as pd
import yaml

from dfolks.data.data import (
    AbstractParser,
    Validator,
)
from dfolks.data.input import (
    load_flat_file,
)


class TstParser(AbstractParser):
    """Test parser class."""

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


def test_parser_class():
    """Test for parser class."""
    expected_output = pd.DataFrame({"Column1": ["A", "B", "C"], "Column2": [1, 2, 3]})
    schema_yaml = """
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
    schema_dict = yaml.safe_load(schema_yaml)
    expected_parsed_output = pd.DataFrame(
        {"Column1": ["A", "B", "C"], "Column2": ["1", "2", "3"]}
    )

    test_parser = TstParser.model_validate(
        {"path": "src/dfolks/data/test/dummy.csv", "load_all": False}
    )
    test_validator = Validator.model_validate(schema_dict)

    assert isinstance(test_parser, AbstractParser)
    pd.testing.assert_frame_equal(test_parser.load(), expected_output)
    pd.testing.assert_frame_equal(test_parser.parse(), expected_output)

    assert isinstance(test_validator, Validator)
    pd.testing.assert_frame_equal(
        test_validator.valid(test_parser.parse()), expected_parsed_output
    )
