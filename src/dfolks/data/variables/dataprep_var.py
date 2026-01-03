from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class DfVariables(BaseModel):
    """Class for variables of dataframe for data extactor."""

    target_db: Optional[str] = None
    target_path: str
    join_type: Optional[str] = None
    join_keys: Optional[List] = None
    schemas: Optional[Dict] = Field(description="schema_for_base_df.", default=None)


class FillnaVariables(BaseModel):
    """Class for variables of fillna operation."""

    column: str
    value: int | float | str | bool


class ImputeVariables(BaseModel):
    """Class for variables of Impute operation."""

    group_cols: List[str]
    impute_cols: List[str]
    value: int | float | str | bool
