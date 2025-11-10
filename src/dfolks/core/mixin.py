"""Mixed in classes.

1) ExternalFileMixin for loading external files.

Need to work:
0) Support for other file types.
"""

from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator

# Prefix for external file to be defined as variables.
FILEPREFIX = "file://"


def _load_by_path(path):
    """Load variables from an external yaml file."""

    # Get absolute path.
    path = str(path)
    f_path = Path(path).resolve().absolute()

    if not f_path.exists():
        raise FileNotFoundError(f"File not found: {f_path}")

    # Raise error if the file type is not YAML.
    if f_path.suffix not in [".yaml", ".yml"]:
        raise ValueError(f"Unsupported file type: {f_path.suffix}")

    # Import variable from a yaml file.
    with open(f_path) as f:
        ext_params = yaml.safe_load(f)

    return ext_params


class ExternalFileMixin(BaseModel):
    """Mixed in class for loading variables from external files.

    pydantic validator is used to load variables from external files.
    if a variable starts with "FILEPREFIX", then it parses variables from the file.
    """

    @model_validator(mode="before")
    def _ext_params(cls, values):
        """Load variables from external files."""
        to_be_loaded = {}
        values = values.copy()

        # Filter variables which start with FILEPREFIX and store in to_be_loaded.
        for k, v in values.items():
            if isinstance(v, str) and v.startswith(FILEPREFIX):
                to_be_loaded[k] = v[len(FILEPREFIX) :]

        # Load variables from the external files and update values.
        for name, path in to_be_loaded.items():
            values[name] = _load_by_path(path)

        return values
