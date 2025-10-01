import re
import pandas as pd


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
