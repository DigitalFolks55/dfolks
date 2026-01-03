"""Data extractor.

Data extractor functions for data preparation workflows.
using normal class registry?

Need to do
0) Data load from various files.
1) Data cleaning.
2) Data transformation.
2-1) Imputing for time series data.
3) Data validation.
"""

from datetime import datetime
from pathlib import Path
from typing import ClassVar, Dict, List, Optional

import pandas as pd
from pydantic import Field

from dfolks.core.classfactory import NormalClassRegistery
from dfolks.data.data import Validator, enforce_dtype, fillna_dataframe_numeric_cols
from dfolks.data.output import __user_dic__
from dfolks.data.variables.dataprep_var import DfVariables, FillnaVariables


class DataExtractor(NormalClassRegistery):
    """Data extractor class."""

    nmclss: ClassVar[str] = "DataExtractor"

    kind: str = "DataExtractor"

    base_df: DfVariables
    join_dfs: List = None
    fillna_data: List = None
    impute_data: Dict = None
    filter_query: str = None
    schema_final_df: Optional[Dict] = Field(
        description="schema_for_final_df.", default=None
    )
    save_final_df: bool = False

    @property
    def variables(self) -> Dict:
        """Return Variables of a pydantic model."""
        return self.model_dump()

    @property
    def get_full_path(self, db: Optional[str], path: str) -> str:
        """Get full path."""
        if db:
            full_path = Path.joinpath(__user_dic__, db, path)
        else:
            full_path = path

        return full_path

    @property
    def get_base_df(self) -> pd.DataFrame:
        """Get base dataframe."""
        v = self.variables

        if v.get("base_df")["target_db"]:
            # Set up folder path to be stored.
            full_path = self.get_full_path(
                v["base_df"]["target_db"], v["base_df"]["target_path"]
            )
        else:
            full_path = v["base_df"]["target_path"]

        if full_path.endswith(".csv"):
            base_df = pd.read_csv(full_path)
        elif full_path.endswith(".parquet"):
            base_df = pd.read_parquet(full_path)
        else:
            raise NotImplementedError("Not implemented yet!")

        if v.get("base_df")["schemas"]:
            schemas = {}
            for col in v["base_df"]["schemas"]:
                schemas[col] = v["base_df"]["schemas"][col]["type"]
            base_df = enforce_dtype(base_df, schemas)

        return base_df

    def merge_join_dfs(self, base_df, list_of_dfs) -> pd.DataFrame:
        """Join dataframes."""
        for join_df in list_of_dfs:
            join_df_dict = DfVariables.model_validate(join_df).model_dump()

            if join_df_dict.get("target_db", None):
                # Set up folder path to be stored.
                full_path = self.get_full_path(
                    join_df_dict["target_db"], join_df_dict["target_path"]
                )
            else:
                full_path = join_df_dict["target_path"]

            if full_path.endswith(".csv"):
                df = pd.read_csv(full_path)
            elif full_path.endswith(".parquet"):
                df = pd.read_parquet(full_path)
            else:
                raise NotImplementedError("Not implemented yet!")

            if join_df_dict.get("schemas", None):
                df = enforce_dtype(df, join_df_dict["schemas"])

            join_type = join_df_dict.get("join_type", "inner")
            join_keys = join_df_dict.get("join_keys", None)

            if join_keys is not None:
                base_df = base_df.merge(df, how=join_type, on=join_keys)

        return base_df

    def fillna_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fillna dataframe based on variables."""
        v = self.variables

        fillna_dict_numeric = {}
        fillna_dict_others = {}

        for fillna_var in v["fillna_data"]:
            fillna_var_dict = FillnaVariables.model_validate(fillna_var).model_dump()

            if (
                fillna_var_dict["column"]
                in df.select_dtypes(include=["number"]).columns
            ):
                fillna_dict_numeric[fillna_var_dict["column"]] = fillna_var_dict[
                    "value"
                ]
            else:
                fillna_dict_others[fillna_var_dict["column"]] = fillna_var_dict["value"]

        if len(fillna_dict_numeric) > 0:
            df = fillna_dataframe_numeric_cols(df, fillna_dict_numeric)
        if len(fillna_dict_others) > 0:
            df = df.fillna(fillna_dict_others)

        return df

    def filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter dataframe based on query."""
        v = self.variables

        df = df.query(v["filter_query"])

        return df

    def extract(self) -> pd.DataFrame:
        """Extract data as dataframe."""
        v = self.variables
        df = self.get_base_df

        if v["join_dfs"]:
            df = self.merge_join_dfs(df, v["join_dfs"])

        if v["fillna_data"]:
            df = self.fillna_df(df)

        if v["filter_query"]:
            df = self.filter_df(df)

        if v["schema_final_df"]:
            df_valid = Validator.model_validate(v["schema_final_df"]).valid(df)
        else:
            df_valid = df

        if v["save_final_df"]:
            cache_folder = Path.joinpath(__user_dic__, "cache")
            if not cache_folder.exists():
                cache_folder.mkdir(parents=True, exist_ok=True)
            exec_time = datetime.now().strftime("%Y%m%d%H%M%S")

            full_path = Path.joinpath(cache_folder, f"cache_dataprep_{exec_time}.csv")

            df_valid.to_csv(full_path, index=False)

        return df_valid
