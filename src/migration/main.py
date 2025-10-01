from dotenv import load_dotenv
import os
import pandas as pd
from typing import List, Dict, Any

load_dotenv() # Reads .env and adds to environment

TABLES_CONFIG: List[Dict[str, Any]] = [
    {'table_name': 'customers', 'file_path': 'bronze/olist_customers_dataset.csv'},
    {'table_name': 'geolocation', 'file_path': 'bronze/olist_geolocation_dataset.csv'},
    {'table_name': 'order_items', 'file_path': 'bronze/olist_order_items_dataset.csv'},
    {'table_name': 'order_payments', 'file_path': 'bronze/olist_order_payments_dataset.csv'},
    {'table_name': 'order_reviews', 'file_path': 'bronze/olist_order_reviews_dataset.csv'},
    {'table_name': 'orders', 'file_path': 'bronze/olist_orders_dataset.csv'},
    {'table_name': 'products', 'file_path': 'bronze/olist_products_dataset.csv'},
    {'table_name': 'sellers', 'file_path': 'bronze/olist_sellers_dataset.csv'},
    {'table_name': 'product_category_name_translation', 'file_path': 'bronze/product_category_name_translation.csv'}
]

def extract_data(tables_config: List[Dict[str, Any]]) -> Dict[str,  pd.DataFrame]:
    """
    Extracts data for all tables defined in the configuration.

    Args:
        tables_config: A list of dicts, each defining a table and its source file.

    Returns:
        A dictionary mapping table names to their extracted pandas DataFrame.
    """
    extracted_data = {}
    print("--- STARTING EXTRACTION PROCESS ---")
    for config in tables_config:
        table_name = config['table_name']
        file_path = config['file_path']

        if not os.path.exists(file_path):
            print(f"Error: File not found for table {table_name} at {file_path}")
            continue

        print(f"Extracting {table_name} data from {os.path.basename(file_path)}")
        df = pd.read_csv(file_path)
        extracted_data[table_name] = df
        print(f"Table {table_name}: Found {len(df)} rows")

        print("--- EXTRACTION PROCESS COMPLETED ---")

        return extracted_data


def transform_data(extracted_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    pass


def load_data(transformed_data: Dict[str, pd.DataFrame]):
    pass

def run_migration():
    """Main function to orchestrate the ETL process."""

    raw_dataframes = extract_data(TABLES_CONFIG)

    for table in raw_dataframes:
        print("---CHECK---")
        print(raw_dataframes[table].head())
        print()

    transformed_dataframes = transform_data(raw_dataframes)
    load_data(transformed_dataframes)


if __name__ == "__main__":
    run_migration()