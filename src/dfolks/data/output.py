"""Save DataFrame with customized data hive.

Need to do
1) Meta file - schema, format; pandas, created date, sample
2) update (incremental, upsert, overwrite etc..), archive, remove
2) other flat, media & model hive
3) pyspark hive & other formats like arvo, orc
4) doc
"""

import logging
import os
from pathlib import Path
from typing import ClassVar, Dict, List

import pandas as pd
from pydantic import ConfigDict

from dfolks.core.classfactory import NormalClassRegistery

# Set up shared logger
logger = logging.getLogger("shared")

# Set up user root directory for data hive
__user_dic__ = Path.joinpath(Path.home(), "DataHive")
__support_file_types__ = ["csv", "parquet"]
__support_write_modes__ = ["overwrite", "append", "upsert", "archive", "incremental"]


class SaveFile(NormalClassRegistery):
    """Class for saving DataFrame to a file."""

    nmclss: ClassVar[str] = "SaveFile"
    df: pd.DataFrame
    file_type: str = "csv"
    file_db: str = None
    file_path: str = None
    primary_keys: List[str] = None
    partition_cols: List[str] = None
    compression: str = None
    write_mode: str = "overwrite"
    schema_evolution: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()

    @property
    def _df(self) -> pd.DataFrame:
        return self.df

    def mode(self, mode: str) -> "SaveFile":
        global __support_write_modes__

        mode = mode.lower()

        if mode not in __support_write_modes__:
            raise ValueError(
                f"Unsupported mode '{mode}'. Choose from {__support_write_modes__}."
            )
        self.write_mode = mode

        return self

    def type(self, file_type: str) -> "SaveFile":
        global __support_file_types__

        file_type = file_type.lower()

        if file_type not in __support_file_types__:
            raise ValueError(
                f"Unsupported file type '{file_type}'. Choose from {__support_file_types__}."
            )
        self.file_type = file_type

        return self

    @property
    def path(self) -> "SaveFile":
        v = self.variables
        file_db = v["file_db"]
        file_path = v["file_path"]

        if file_db is not None:
            logger.info(
                f"DataFrame will be stored in DataHive with {file_db}/{file_path}"
            )

            global __user_dic__

            # Set up folder path to be stored
            folder_path = Path.joinpath(__user_dic__, file_db)

            # Check if the folder exists and create it if necessary
            if not folder_path.exists():
                folder_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Folder created: {folder_path}")
            else:
                logger.info(f"Folder already exists: {folder_path}")

            final_path = Path.joinpath(folder_path, file_path)
        elif file_db is None and file_path is not None:
            logger.info(f"DataFrame will be stored in the defined path: {file_path}")
            final_path = file_path
        else:
            raise ValueError("Either 'file_db' or 'file_path' should be provided.")

        return final_path

    def write_func(self, df: pd.DataFrame, path: str) -> None:
        v = self.variables
        if v["file_type"] == "csv":
            df.to_csv(path, header=True, index=False)
        else:
            df.to_parquet(
                path,
                compression=v["compression"],
                partition_cols=v["partition_cols"],
                index=False,
            )

    def save(self) -> None:
        v = self.variables
        df = self._df
        file_path = self.path

        # Determine file format
        read_func = pd.read_csv if v["file_type"] == "csv" else pd.read_parquet

        existing_df = (
            read_func(file_path) if os.path.exists(file_path) else pd.DataFrame()
        )

        if v["write_mode"] in ["append", "incremental", "upsert"]:
            if v["primary_keys"] and not all(
                pk in existing_df.columns for pk in v["primary_keys"]
            ):
                raise ValueError(
                    f"Primary keys {v["primary_keys"]} not found in existing data."
                )

            if v["schema_evolution"]:
                # Align columns
                for col in existing_df.columns:
                    if col not in df.columns:
                        df[col] = None
                for col in df.columns:
                    if col not in existing_df.columns:
                        existing_df[col] = None
                df = df[existing_df.columns.tolist()]
            else:
                if v["primary_keys"] and not all(
                    pk in df.columns for pk in v["primary_keys"]
                ):
                    raise ValueError(
                        f"Primary keys {v["primary_keys"]} not found in new data."
                    )

        if self.write_mode == "overwrite" or not os.path.exists(file_path):
            result_df = df

        elif self.write_mode == "append":
            result_df = pd.concat([existing_df, df], ignore_index=True)

        elif self.write_mode == "incremental":
            existing_keys = existing_df[v["primary_keys"]].drop_duplicates()
            new_df = df.merge(
                existing_keys, on=v["primary_keys"], how="left", indicator=True
            )
            result_df = pd.concat(
                [
                    existing_df,
                    new_df[new_df["_merge"] == "left_only"].drop(columns="_merge"),
                ],
                ignore_index=True,
            )

        elif self.write_mode == "upsert":
            # Remove rows in existing_df that will be replaced
            merged = existing_df.merge(
                df[v["primary_keys"]], on=v["primary_keys"], how="left", indicator=True
            )
            existing_filtered = merged[merged["_merge"] == "left_only"].drop(
                columns="_merge"
            )

            result_df = pd.concat([existing_filtered, df], ignore_index=True)

        # Save final result
        self.write_func(result_df, file_path)

        logger.info(f"DataFrame is saved in {file_path} with mode '{self.write_mode}'.")
