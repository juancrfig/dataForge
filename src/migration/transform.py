import pandas as pd
from utils import _sanitize_strings


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
    STRING_COLS_TO_SANITIZE = ['customer_id', 'customer_unique_id', 'customer_city', 'customer_state']

    # 1. Security Cleaning: Strip dangerous injection characters from all string columns
    for col in STRING_COLS_TO_SANITIZE:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].apply(_sanitize_strings)

    # 2. Rename column names
    COLUMN_MAPPING = {
        'customer_id': 'id',
        'customer_unique_id': 'unique_id',
        'customer_city': 'city',
        'customer_state': 'state',
        'customer_zip_code_prefix': 'zip_code_prefix',
    }
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # 3. Type Optimization  for Categorical Identifiers
    for col in ['customer_zip_code_prefix', 'customer_city', 'customer_state']:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # 4. Standardization / Cleaning
    if 'customer_city' in df.columns:
        df['customer_city'] = df['customer_city'].str.lower().str.strip()

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

    STRING_COLS_TO_SANITIZE = ['geolocation_city', 'geolocation_state']

    # 1. Security Cleaning: Strip dangerous injection characters from all string columns
    for col in STRING_COLS_TO_SANITIZE:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].apply(_sanitize_strings)
            # Standardization
            df[col] = df[col].str.lower().str.strip()
            # Type Optimization
            df[col] = df[col].astype('category')

    if 'geolocation_zip_code_prefix' in df.columns and df['geolocation_zip_code_prefix'].dtype != 'category':
        df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype('category')


    # 2. Aggregation: Create the final one-to-one lookup table
    # Group by the zip code prefix and calculate the mean lat/lng
    agg_df = df.groupby('geolocation_zip_code_prefix', observed=True).agg(
        avg_lat =('geolocation_lat', 'mean'),
        avg_lng =('geolocation_lng', 'mean'),
        state_mode=('geolocation_state', lambda x: x.mode()[0]),
    ).reset_index()


    # 3. Rename column names
    COLUMN_MAPPING = {
        'geolocation_zip_code_prefix': 'zip_code_prefix',
        'avg_lat': 'latitude',
        'avg_lng': 'longitude',
        'state_mode': 'state',
    }
    agg_df.rename(columns=COLUMN_MAPPING, inplace=True)

    agg_df['state'] = agg_df['state'].astype('category')

    return agg_df


def _transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and type optimization to the order items DataFrame.

    Args:
        df: The raw order items DataFrame.

    Returns:
        The cleaned order items DataFrame.
    """




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
