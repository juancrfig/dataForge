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


def _transform_order_payments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and feature engineering to the 'order_payments' DataFrame.

    This table is aggregated to the 'order_id' level to summarize payment details into a single row per order,
    creating a clean feature lookup.

    Args:
        df: The raw 'order_payments' DataFrame.

    Returns:
        The transformed and aggregated DataFrame ready for joining with te 'orders' table.
    """
    # Security & Cleaning
    df = _clean_string_columns(_sanitize_dataframe(df))
    # Initial Type Optimization
    if 'payment_type' in df.columns:
        df['payment_type'] = df['payment_type'].astype('category')

    # Aggregation: Summarize payments to the Order level (one row per 'order_id')
    agg_df = df.groupby('order_id', observed=True).agg(
        total_payment_value=('payment_value', 'sum'),
        max_installments=('payment_installments', 'max'),
        payment_chunk_count=('payment_sequential', 'count'),
        # Determine the primary payment type by frequency (mode)
        main_payment_type=('payment_type', lambda x: x.mode()[0] if not x.mode().empty else None)
    ).reset_index()

    # Renaming
    COLUMN_MAPPING = {
        'order_id': 'id',
        'total_payment_value': 'total_paid',
        'max_installments': 'num_payments',
        'main_payment_type': 'payment_type_mode',
    }
    agg_df.rename(columns=COLUMN_MAPPING, inplace=True)

    agg_df = _set_category_type(df)

    return agg_df

def _transform_order_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning, type casting, and feature engineering to the 'order_reviews' DataFrame.

    This function casts date objects, cleans text, and aggregates review data to the order level
    (mean score, review count) for a clean Gold Layer lookup.

    Args:
        df: The raw 'order_reviews' DataFrame.

    Returns:
        The transformed and aggregated DataFrame ready for joining with the 'orders' table.
    """
    # 1. Security & Cleaning (Sanitize and clean all string columns)
    df = _clean_string_columns(_sanitize_dataframe(df))

    # 2. Datetime Casting
    date_cols = ['review_creation_date', 'review_answer_timestamp']
    existing_date_cols = [col for col in date_cols if col in df.columns]
    if existing_date_cols:
        df[existing_date_cols] = df[existing_date_cols].apply(pd.to_datetime, errors='coerce')

    # Handle Null comments
    text_cols = ['review_comment_title', 'review_comment_message']
    existing_text_cols = [col for col in text_cols if col in df.columns]
    if existing_text_cols:
        df[existing_text_cols] = df[existing_text_cols].fillna('')

    # Aggregation to Order Level
    agg_df = df.groupby('order_id', observed=True).agg(
        review_count=('review_id', 'count'),
        avg_review_score=('review_score', 'mean'),
        latest_review_date=('review_creation_date', 'max'),
        latest_answer_date=('review_answer_timestamp', 'max'),
    ).reset_index()

    # 4. Renamin
    COLUMN_MAPPING = {
        'order_id': 'id',
        'review_count': 'num_reviews',
        'avg_review_score': 'score_avg',
        'latest_review_date': 'review_date_latest',
        'latest_answer_date': 'answer_date_latest',
    }
    agg_df.rename(columns=COLUMN_MAPPING, inplace=True)

    # 5. Final Type Optimization
    agg_df = _set_category_type(agg_df)

    return agg_df


def _transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and feature engineering to the 'orders' DataFrame.

    This function casts timestamp columns to datetime objects, engineers several key
    time-based features (e.g., delivery time, days to ship), and standardizes
    column names for the Gold Layer.

    Args:
        df: The raw 'orders' DataFrame.

    Returns:
        The transformed 'orders' DataFrame with optimized types and new features.
    """
    # 1. Standard Cleaning & Initial Type Optimization
    df = _clean_string_columns(_sanitize_dataframe(df))

    # 2. Safe Datetime Casting
    date_cols = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    existing_date_cols = [col for col in date_cols if col in df.columns]
    if existing_date_cols:
        df[existing_date_cols] = df[existing_date_cols].apply(pd.to_datetime, errors='coerce')

    # 3. Renaming (Done before feature engineering for cleaner access)
    COLUMN_MAPPING = {
        'order_id': 'id',
        'customer_id': 'customer',
        'order_status': 'status',
        'order_purchase_timestamp': 'purchase',
        'order_approved_at': 'approved',
        'order_delivered_carrier_date': 'carrier_delivery',
        'order_delivered_customer_date': 'customer_delivery',
        'order_estimated_delivery_date': 'estimated_delivery',
    }
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # 4. Feature Engineering: Calculate time deltas in days
    # These metrics are crucial for business intelligence and performance analysis.
    # The .dt.days accessor correctly handles NaT (missing dates) by producing NaN.
    df['delivery_time_days'] = (df['customer_delivery'] - df['purchase']).dt.days
    df['approval_time_days'] = (df['approved'] - df['purchase']).dt.days

    # Negative values indicate a late delivery.
    df['delivery_lateness_days'] = (df['estimated_delivery'] - df['customer_delivery']).dt.days

    df = _set_category_type(df)

    return df


def _transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning, feature engineering, and type correction to the 'products' DataFrame.

    This function handles missing values by imputing or filling, corrects data types from float
    to a nullable integer, engineers a 'volume' feature, and standardizes column names.

    Args:
        df: The raw 'products' DataFrame.

    Returns:
        The transformed 'products' DataFrame.
    """
    # 1. Standard Cleaning
    df = _clean_string_columns(_sanitize_dataframe(df))

    # 2. Feature Engineering: Calculate volume before imputing nulls
    df['product_volume_cm3'] = df['product_length_cm'] * df['product_height_cm'] * df['product_width_cm']

    # 3. Handle Missing Values
    # Impute the few missing dimensional metrics with the median for robustness.
    dimensional_cols = [
        'product_weight_g', 'product_length_cm', 'product_height_cm',
        'product_width_cm', 'product_volume_cm3'
    ]
    for col in dimensional_cols:
        if col in df.columns:
            median_val = df[col].median()
            df[col].fillna(median_val)

    # Fill missing category with a placeholder; it's a feature, not a metric.
    if 'product_category_name' in df.columns:
        df['product_category_name'] = df['product_category_name'].fillna('unknown')

    # 4. Correct Data Types
    # These columns represent counts or lengths and should be integers.
    # We use pandas' nullable 'Int64' type to handle any potential nulls.
    integer_cols = [
        'product_name_lenght', 'product_description_lenght', 'product_photos_qty'
    ]
    for col in integer_cols:
        if col in df.columns:
            df[col] = df[col].astype('Int64')

    # 5. Renaming
    COLUMN_MAPPING = {
        'product_id': 'id',
        'product_category_name': 'category',
        'product_name_lenght': 'name_length',
        'product_description_lenght': 'description_length',
        'product_photos_qty': 'photos_qty',
        'product_weight_g': 'weight_g',
        'product_length_cm': 'length_cm',
        'product_height_cm': 'height_cm',
        'product_width_cm': 'width_cm',
        'product_volume_cm3': 'volume_cm3'
    }
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # 6. Final Type Optimization for IDs and categorical features
    df = _set_category_type(df)

    return df


def _transform_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and type optimization to the 'sellers' DataFrame.

    This function standardizes column names and converts all columns to memory-efficient
    dtypes suitable for joining.

    Args:
        df: The raw 'sellers' DataFrame.

    Returns:
        The transformed 'sellers' DataFrame.
    """
    # 1. Standard Cleaning
    df = _clean_string_columns(_sanitize_dataframe(df))

    # 2. Renaming
    COLUMN_MAPPING = {
        'seller_id': 'id',
        'seller_zip_code_prefix': 'zip_code_prefix',
        'seller_city': 'city',
        'seller_state': 'state',
    }
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # 3. Final Type Optimization
    df = _set_category_type(df)

    return df


def _transform_category_translation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Silver Layer cleaning and type optimization to the 'product_category_name_translation' DataFrame.

    This function prepares the translation table for use as a clean lookup map by standardizing
    names and optimizing data types.

    Args:
        df: The raw category translation DataFrame.

    Returns:
        The transformed DataFrame.
    """
    # 1. Standard Cleaning (Crucial for reliable joins)
    df = _clean_string_columns(_sanitize_dataframe(df))

    # 2. Renaming
    COLUMN_MAPPING = {
        'product_category_name': 'category',
        'product_category_name_english': 'category_english',
    }
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # 3. Final Type Optimization
    df = _set_category_type(df)

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
        'order_payments': _transform_order_payments,
        'order_reviews': _transform_order_reviews,
        'orders': _transform_orders,
        'products': _transform_products,
        'sellers': _transform_sellers,
        'product_category_name_translation': _transform_category_translation
    }

    for table_name, df in transformed_data.items():
        if table_name in TRANSFORMATION_MAP:
            transform_func = TRANSFORMATION_MAP[table_name]

            print(f"Applying Silver Layer to: {table_name}")
            transformed_data[table_name] = transform_func(df)
        else:
            print(f"No transformation applied for {table_name}")

    return transformed_data
