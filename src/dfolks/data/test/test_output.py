import os
import tempfile

import pandas as pd
import pytest

from dfolks.data.output import SaveFile

__support_write_modes__ = ["overwrite", "append", "incremental", "upsert"]
__support_file_types__ = ["csv", "parquet"]


@pytest.fixture
def sample_df():
    return pd.DataFrame({"id": [1, 2], "value": ["A", "B"]})


@pytest.fixture
def temp_dir():
    """Create a temporary folder for tests (auto-cleaned)."""
    with tempfile.TemporaryDirectory() as tempdir:
        yield tempdir


@pytest.fixture
def existing_df_path(temp_dir):
    file_path = os.path.join(temp_dir, "existing.csv")
    df = pd.DataFrame({"id": [1], "value": ["X"]})
    df.to_csv(file_path, index=False)
    return file_path


def test_overwrite_mode_creates_new_file(temp_dir, sample_df):
    file_path = os.path.join(temp_dir, "test.csv")

    saver = SaveFile(df=sample_df, file_path=file_path)
    saver.mode("overwrite").save()

    assert os.path.exists(file_path)
    written_df = pd.read_csv(file_path)
    pd.testing.assert_frame_equal(sample_df, written_df)


def test_append_mode_adds_rows(existing_df_path, sample_df):
    saver = SaveFile(df=sample_df, file_path=existing_df_path, primary_keys=["id"])
    saver.mode("append").save()

    written_df = pd.read_csv(existing_df_path)
    assert len(written_df) == 3  # existing (1 row) + new (2 rows)
    assert set(written_df.columns) == {"id", "value"}


def test_incremental_mode_adds_only_new_keys(temp_dir):
    existing_path = os.path.join(temp_dir, "incremental.csv")
    existing = pd.DataFrame({"id": [1, 2], "value": ["A", "B"]})
    existing.to_csv(existing_path, index=False)

    new = pd.DataFrame({"id": [2, 3], "value": ["B2", "C"]})
    saver = SaveFile(df=new, file_path=existing_path, primary_keys=["id"])
    saver.mode("incremental").save()

    written_df = pd.read_csv(existing_path)
    assert sorted(written_df["id"]) == [1, 2, 3]
    assert "B2" not in written_df["value"].values  # id=2 not replaced


def test_upsert_mode_replaces_existing_rows(temp_dir):
    existing_path = os.path.join(temp_dir, "upsert.csv")
    existing = pd.DataFrame({"id": [1, 2], "value": ["A", "B"]})
    existing.to_csv(existing_path, index=False)

    new = pd.DataFrame({"id": [2, 3], "value": ["B2", "C"]})
    saver = SaveFile(df=new, file_path=existing_path, primary_keys=["id"])
    saver.mode("upsert").save()

    written_df = pd.read_csv(existing_path)
    expected = pd.DataFrame({"id": [1, 2, 3], "value": ["A", "B2", "C"]})
    pd.testing.assert_frame_equal(
        written_df.sort_values("id").reset_index(drop=True), expected
    )


def test_schema_evolution_adds_missing_columns(temp_dir):
    existing_path = os.path.join(temp_dir, "schema.csv")
    existing = pd.DataFrame({"id": [1], "value": ["A"]})
    existing.to_csv(existing_path, index=False)

    new = pd.DataFrame({"id": [2], "new_col": ["B"]})
    saver = SaveFile(
        df=new, file_path=existing_path, primary_keys=["id"], schema_evolution=True
    )
    saver.mode("upsert").save()

    written_df = pd.read_csv(existing_path)
    assert "new_col" in written_df.columns
    assert "value" in written_df.columns


def test_unsupported_mode_raises_error(sample_df):
    saver = SaveFile(df=sample_df)
    with pytest.raises(ValueError):
        saver.mode("invalid_mode")


def test_unsupported_file_type_raises_error(sample_df):
    saver = SaveFile(df=sample_df)
    with pytest.raises(ValueError):
        saver.type("invalid_type")


def test_primary_key_missing_in_existing_data(temp_dir):
    existing_path = os.path.join(temp_dir, "existing.csv")
    existing = pd.DataFrame({"id": [1], "value": ["A"]})
    existing.to_csv(existing_path, index=False)

    new = pd.DataFrame({"value": ["B"]})
    saver = SaveFile(df=new, file_path=existing_path, primary_keys=["id"])
    saver.mode("upsert")

    with pytest.raises(ValueError):
        saver.save()


def test_primary_key_missing_in_new_data(temp_dir):
    existing_path = os.path.join(temp_dir, "existing.csv")
    existing = pd.DataFrame({"id": [1], "value": ["A"]})
    existing.to_csv(existing_path, index=False)

    new = pd.DataFrame({"value": ["B"]})
    saver = SaveFile(df=new, file_path=existing_path, primary_keys=["id"])
    saver.mode("incremental")

    with pytest.raises(ValueError):
        saver.save()
