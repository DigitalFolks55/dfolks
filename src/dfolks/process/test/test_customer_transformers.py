import numpy as np
import pandas as pd

from dfolks.process.custom_transformers import RemoveNanColsTransformer


def test_transform_removes_columns_above_threshold():
    """Columns with NaN ratio >= threshold are removed."""
    df = pd.DataFrame(
        {
            "A": [1, 2, np.nan, 4],  # 25% NaN
            "B": [np.nan, np.nan, 3, 4],  # 50% NaN
            "C": [np.nan, np.nan, np.nan, 1],  # 75% NaN
        }
    )

    transformer = RemoveNanColsTransformer(threshold=0.5)
    result = transformer.transform(df)

    # A (25%) kept (< threshold) & B (50%), C (75%) removed
    assert list(result.columns) == ["A"]


def test_transform_with_stricter_threshold_keeps_all():
    """If threshold is high, all columns stay."""
    df = pd.DataFrame(
        {
            "A": [1, np.nan, 3, 4],
            "B": [np.nan, 2, np.nan, 4],
        }
    )

    transformer = RemoveNanColsTransformer(threshold=0.8)
    result = transformer.transform(df)

    # Both columns have < 0.8 NaN ratio
    assert list(result.columns) == ["A", "B"]
