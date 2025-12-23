"""Utils."""

from typing import Dict, List


def extract_primary_keys(variables: Dict) -> List:
    """Extract primary key columns from variables."""

    primary_keys = []

    for col, dtype in variables.get("schemas", {}).items():
        if dtype.get("primary_key", False):
            if dtype.get("new_column", False):
                primary_keys.append(dtype["new_column"])
            else:
                primary_keys.append(col)

    return primary_keys


def extract_partition_cols(variables: Dict) -> List:
    """Extract partition columns from variables."""
    partition_keys = []

    for col, dtype in variables.get("schemas", {}).items():
        if dtype.get("partition_key", False):
            if dtype.get("new_column", False):
                partition_keys.append(dtype["new_column"])
            else:
                partition_keys.append(col)

    return partition_keys
