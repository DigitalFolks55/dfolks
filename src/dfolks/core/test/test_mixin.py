"""Test for chain."""

from typing import Dict

from dfolks.core.mixin import ExternalFileMixin


class TestExternalFileMixin(ExternalFileMixin):
    """Test class for ExternalFileMixin."""

    ext_yaml: Dict


def test_external_file_mixin():
    """Test ExternalFileMixin functionality."""
    test_instance = TestExternalFileMixin(
        **{"ext_yaml": "file://src/dfolks/core/test/dummy/dummy_ext_params.yaml"}
    )

    assert test_instance.ext_yaml == {
        "param1": "value1",
        "param2": 2,
        "param3": [1, 2, 3],
    }
