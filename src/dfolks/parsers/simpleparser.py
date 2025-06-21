"""Simple parser for flat files."""

from typing import ClassVar, Dict, Optional

from dfolks.data.data import AbstractParser
from dfolks.data.input import load_flat_file


class SimpleParser(AbstractParser):
    """Simple parser for flat files.

    Variables
    ----------
    source: str
        Source type - file.  # Need to add more
    source_path: str
        Source path.
    load_all: bool = False
        Load all files in the directory, if path is a directory.
    sep: Optional[str] = None
        Separator for flat files. It is optional.
    ----------
    """

    prcls: ClassVar[str] = "SimpleParser"

    source: str
    source_path: str
    load_all: bool = False
    sep: Optional[str] = None

    @property
    def variables(self) -> Dict:
        return super().variables

    def parse(self):
        v = self.variables
        if v["source"] == "file":
            df = load_flat_file(path=v["source_path"], load_all=v["load_all"])

        return df
