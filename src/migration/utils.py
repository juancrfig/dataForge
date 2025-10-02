import re
import pandas as pd
from typing import Union


def _sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strips dangerous characters used in common injection attacks from all columns of type object in a DataFrame.
    This is a basic, proactive security measure for raw DatFrames.

    Args:
        df: The input DataFrame.

    Returns:
        The sanitized DataFrame.
    """
    def _sanitize_value(s):
        if pd.isna(s) or not isinstance(s, str):
            return s
        return re.sub(r'[";\'`()\[\]\{\}<>\-\-#]', '', s)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(_sanitize_value)

    return df


def _clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a DataFrame and cleans its string columns by converting them
    to lowercase and removing leading/trailing whitespace.

    Args:
         df: The input DataFrame.

    Returns:
        The cleaned DataFrame.
    """
    df_cleaned = df.copy()

    string_cols = df_cleaned.select_dtypes(include='object').columns

    for col in string_cols:
        df_cleaned[col] = df_cleaned[col].str.lower().str.strip()

    return df_cleaned


# --- Category Dtype Optimization Constants ---
MAX_UNIQUE_RATIO = 0.5
MAX_UNIQUE_COUNT = 50_000

def _set_category_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set 'category' as dtype for columns that passed a test.

    Args:
        df: The input DataFrame.

    Returns:
        The DataFrame with columns converted to 'category' where thresholds were met.
    """
    for col in df.columns:
        if _should_convert_to_category(df[col], MAX_UNIQUE_RATIO, MAX_UNIQUE_COUNT):
            df[col] = df[col].astype('category')

    return df


def _should_convert_to_category(
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
    n_unique = series.nunique(dropna=True)
    n_rows = len(series)

    if n_rows == 0:
        return False

    unique_ratio = n_unique / n_rows

    # 3. Apply Thresholds
    if unique_ratio <= max_ratio and n_unique <= max_count:
        return True

    return False