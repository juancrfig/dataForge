from typing import Any
import pandas as pd
import os

def extract_data(tables_config: list[dict[str, Any]]) -> dict[str,  pd.DataFrame]:
    """
    Extracts data for all tables defined in the configuration.

    Args:
        tables_config: A list of dicts, each defining a table and its source file.

    Returns:
        A dictionary mapping table names to their extracted pandas DataFrame.
    """
    extracted_data = {}
    for config in tables_config:
        table_name = config['table_name']
        file_path = config['file_path']

        if not os.path.exists(file_path):
            print(f"Error: File not found for table {table_name} at {file_path}")
            continue

        df = pd.read_csv(file_path)
        extracted_data[table_name] = df

    return extracted_data