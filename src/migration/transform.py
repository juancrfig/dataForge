import pandas as pd
from utils import _sanitize_dataframe, _set_category_type, _clean_string_columns


# --- Specific Table Transformations ---

def _transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and type optimization to the customer DataFrame.

    This process ensures categorical identifier columns are stored using the memory-efficient 'category' dtype,
    which significantly improves performance for grouping and merging operations later in the pipeline.

    Args:
        df: The raw customer DataFrame.

    Returns:
        The transformed customer DataFrame with optimized dtypes.
    """
    # Security & Cleaning
    df = _clean_string_columns(_sanitize_dataframe(df))

    # Rename column names
    COLUMN_MAPPING = {
        'customer_id': 'id',
        'customer_unique_id': 'unique_id',
        'customer_city': 'city',
        'customer_state': 'state',
        'customer_zip_code_prefix': 'zip_code_prefix',
    }
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # Type Optimization  for Categorical Identifiers
    df = _set_category_type(df)

    # 5. Handle potential null values, although not required for this dataset.

    return df


def _transform_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and type optimization to the customer DataFrame.

    This table is aggregated to provide a single, average lat(lng pair per zip code prefix, optimizing it for a
    one-to-one lookup in the Gold Layer.

    Args:
        df: The raw geolocation DataFrame.

    Returns:
        The aggregated and cleaned geolocation lookup DataFrame.
    """
    # Security & Cleaning
    df = _clean_string_columns(_sanitize_dataframe(df))

    # Initial Type Optimization (Improves groupby performance)
    df = _set_category_type(df)


    # 2. Aggregation: Create the final one-to-one lookup table
    # Group by the zip code prefix and calculate the mean lat/lng
    agg_df = df.groupby('geolocation_zip_code_prefix', observed=True).agg(
        avg_lat=('geolocation_lat', 'mean'),
        avg_lng=('geolocation_lng', 'mean'),
        state_mode=('geolocation_state', lambda x: x.mode()[0] if not x.mode().empty else None),
    ).reset_index()

    # Rename column names
    COLUMN_MAPPING = {
        'geolocation_zip_code_prefix': 'zip_code_prefix',
        'avg_lat': 'latitude',
        'avg_lng': 'longitude',
        'state_mode': 'state',
    }
    agg_df.rename(columns=COLUMN_MAPPING, inplace=True)

    # Final Type Optimization on the new aggregated DataFrame
    agg_df = _set_category_type(agg_df)

    return agg_df


def _transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and type optimization to the order items DataFrame.

    Args:
        df: The raw order items DataFrame.

    Returns:
        The cleaned order items DataFrame.
    """
    # Security & Cleaning
    df = _clean_string_columns(_sanitize_dataframe(df))

    # Initial Type Optimization (Improves groupby performance)
    df = _set_category_type(df)

    # Rename column names
    COLUMN_MAPPING = {
        'order_id': 'id',
        'order_item_id': 'item',
        'product_id': 'product',
        'seller_id': 'seller',
        'freight_value': 'freight'
    }
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    return df



def transform_data(extracted_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Orchestrates all Silver Layer transformations across the extracted DataFrames using
    a Function Dispatch Table for maintainability and scalability (OCP).

    Args:
         extracted_data: A dictionary mapping table names to their raw pandas DataFrame.

    Returns:
        A dictionary mapping table names to their transformed pandas DataFrame.
    """
    transformed_data = extracted_data.copy()

    # -- FUNCTION DISPATCH TABLE --
    # Maps table names to their specific transformation function.
    TRANSFORMATION_MAP = {
        'customers': _transform_customers,
        'geolocation': _transform_geolocation,
        'order_items': _transform_order_items,
        # Future tables go here
    }

    for table_name, df in transformed_data.items():
        if table_name in TRANSFORMATION_MAP:
            transform_func = TRANSFORMATION_MAP[table_name]

            print(f"Applying Silver Layer to: {table_name}")
            transformed_data[table_name] = transform_func(df)
        else:
            print(f"No transformation applied for {table_name}")

    return transformed_data
