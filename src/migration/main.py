from dotenv import load_dotenv
import os
import pandas as pd
from typing import Any
from config import TABLES_CONFIG
from extraction import extract_data
from transform import transform_data
from load import load_data

load_dotenv()


def main():
    """Main function to orchestrate the ETL process."""

    raw_dataframes = extract_data(TABLES_CONFIG)
    transformed_dataframes = transform_data(raw_dataframes)
    print("TRANSFORMED DATAFRAMES", transformed_dataframes)
    load_data(transformed_dataframes)


if __name__ == "__main__":
    main()