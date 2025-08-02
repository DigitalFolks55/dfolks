"""Save DataFrame with customized data hive.

Need to do
1) Meta file - schema, format; pandas, created date, sample
2) update (incremental, upsert, overwrite etc..), archive, remove
2) other flat, media & model hive
3) pyspark hive & other formats like arvo, orc
4) doc
"""

import logging
from pathlib import Path
from typing import List

import pandas as pd

# Set up shared logger
logger = logging.getLogger("shared")

# Set up user root directory for data hive
__user_dic__ = Path.joinpath(Path.home(), "DataHive")


def save_to_parquet(
    df: pd.DataFrame,
    db: str,
    path: str,
    compression: str = None,
    partition_cols: List = None,
) -> None:
    """Save DataFrame to a parquet file.

    if db is defined, then DataFrame will be stored in PqHive with {db}/{path}.
        path would not have an extension.
    Otherwise, DataFrame will be stored in the defined path: {path}.
        path should have an extension.

    Inputs
    ----------
    df: input DataFrame.
    db: database name to be stored.
    path: table name or full file path to be stored.
    compression: compression method.
    partition_cols: partition columns for parquet file.
    """

    if db is not None:
        logger.info(f"DataFrame will be stored in PqHive with {db}/{path}")

        global __user_dic__

        process_time = pd.Timestamp.now(tz="Asia/Seoul")
        process_time = process_time.strftime("%Y-%m-%d %H:%M:%S")

        # Set up folder path and meta path to be stored
        folder_path = Path.joinpath(__user_dic__, "PqHive", db)
        meta_path = Path.joinpath(__user_dic__, "PqHive", "meta")

        # Check if the folder exists and create it if necessary
        if not folder_path.exists():
            folder_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Folder created: {folder_path}")
        else:
            logger.info(f"Folder already exists: {folder_path}")

        # Save DataFrame into parquet file
        full_path = Path.joinpath(folder_path, f"{path}.parquet")
        df.to_parquet(
            full_path,
            engine="auto",
            compression=compression,
            partition_cols=partition_cols,
        )
        logger.info(f"DataFrame is saved in {full_path}")

        # Save meta file
        logger.info(f"Meta file will be stored in {meta_path}")
        df_name = f"db.{path}"
        df_schema = pd.DataFrame(
            {"column": df.columns, "datatype": df.dtypes}
        ).reset_index(drop=True)
        df_format = "pandas"
        df_version = 0

        df_meta = {
            "name": df_name,
            "version": df_version,
            "schema": df_schema,
            "format": df_format,
            "created": process_time,
        }
        full_meta_path = Path.joinpath(meta_path, df_name, f"v{df_version}.pkl")
        df_meta.to_pickle(full_meta_path)
        logger.info(f"Meta file is saved in {full_meta_path}")
    else:
        logger.info(f"DataFrame will be stored in the defined path: {path}")
        df.to_parquet(
            path, engine="auto", compression=compression, partition_cols=partition_cols
        )
        logger.info(f"DataFrame is saved in {path}")


def save_to_flatfile(df: pd.DataFrame, db: str, path: str, type: str = "csv") -> None:
    """Save DataFrame to a flat file.

    if db is defined, then DataFrame will be stored in FlatHive with {db}/{path}.
        path would not have an extension.
    Otherwise, DataFrame will be stored in the defined path: {path}.
        path should have an extension.

    Inputs
    ----------
    df: input DataFrame.
    db: database name to be stored.
    path: table name or full file path to be stored.
    """
    if db is not None:
        logger.info(f"DataFrame will be stored in FlatHive with {db}/{path}")

        global __user_dic__

        # Set up folder path to be stored
        folder_path = Path.joinpath(__user_dic__, "FlatHive", db)

        # Check if the folder exists and create it if necessary
        if not folder_path.exists():
            folder_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Folder created: {folder_path}")
        else:
            logger.info(f"Folder already exists: {folder_path}")

        # Save DataFrame into csv file
        if type == "csv":
            full_path = Path.joinpath(folder_path, f"{path}.csv")
            df.to_csv(full_path, index=False)
        else:
            raise NotImplementedError

        logger.info(f"DataFrame is saved in {full_path}")
    else:
        logger.info(f"DataFrame will be stored in the defined path: {path}")
        df.to_csv(path, index=False)
        logger.info(f"DataFrame is saved in {path}")
