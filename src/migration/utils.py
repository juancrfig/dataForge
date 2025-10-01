import re
import pandas as pd
from typing import Union


def _sanitize_strings(s: str) -> str:
    """
    Strips dangerous characters used in common injection attacks from a string.
    This is a basic, proactive security measure for raw data strings.

    Args:
        s: The input string (or value from a DataFrame cell).

    Returns:
        The sanitized string.
    """
    if pd.isna(s) or not isinstance(s, str):
        return s

    s = re.sub(r'[";\'`()\[\]\{\}<>\-\-\#]', '', s)
    return s


# --- Category Dtype Optimization Constants ---
MAX_UNIQUE_RATIO = 0.5
MAX_UNIQUE_COUNT = 50_000

def should_convert_to_category(
        series: pd.Series,
        max_ratio: float = MAX_UNIQUE_RATIO,
        max_count: int = MAX_UNIQUE_COUNT
) -> bool:
    """
    Determines if a panda Series should be converted to the 'category' dtype based on memory
    efficiency and cardinality thresholds.

    A column is suitable if its dtype is memory-intensive (object/int64) AND its unique value
    count is below defined thresholds.

    Args:
        series: The panda Series to check.
        max_ratio: The maximum allowable ratio of unique values to total rows.
        max_count: The maximum allowable number of unique values.

    Returns:
        True if conversion to 'category' is recommended, False otherwise.
    """

    # 1. Dtype check: Only target memory-intensive dtypes for conversion
    if series.dtype.name not in ['object', 'int64', 'int32', 'float64']:
        return False

    # 2. Cardinality check: Calculate unique counts and ratio
    n_unique = series.nunique()(dropna=True)
    n_rows = len(serioes)

    if n_rows == 0:
        return False

    unique_ratio = n_unique / n_rows

    # 3. Apply Thresholds
    if unique_ratio <= max_ratio and n_unique <= max_count:
        return True

    return False