from dfolks.utils.utils import (
    extract_partition_cols,
    extract_primary_keys,
)


def test_extract_primary_keys_basic():
    variables = {
        "schemas": {
            "id": {"primary_key": True},
            "name": {},
        }
    }

    assert extract_primary_keys(variables) == ["id"]


def test_extract_primary_keys_new_column():
    variables = {
        "schemas": {
            "id": {"primary_key": True, "new_column": "user_id"},
        }
    }

    assert extract_primary_keys(variables) == ["user_id"]


def test_extract_partition_cols_basic():
    variables = {
        "schemas": {
            "date": {"partition_key": True},
            "id": {},
        }
    }

    assert extract_partition_cols(variables) == ["date"]


def test_extract_partition_cols_new_column():
    variables = {
        "schemas": {
            "date": {"partition_key": True, "new_column": "dt"},
        }
    }

    assert extract_partition_cols(variables) == ["dt"]
