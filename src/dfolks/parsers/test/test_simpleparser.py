"""Test for simpleparser.

Need to do:
"""

import pandas as pd

from dfolks.parsers.simpleparser import SimpleParser


def test_simple_parser():
    parser = SimpleParser(
        source="file",
        source_path="src/dfolks/parsers/test/dummy/dummy.csv",
        sep=",",
    )
    df = parser.parse()

    assert parser.nmclss == "SimpleParser"
    assert parser.source == "file"
    assert parser.source_path == "src/dfolks/parsers/test/dummy/dummy.csv"
    assert parser.sep == ","
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (3, 2)
