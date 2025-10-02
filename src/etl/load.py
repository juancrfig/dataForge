import os
import pandas as pd
from sqlalchemy import create_engine, text

def load_data(dataframes: dict[str, pd.DataFrame]):
    """
    Loads transformed DataFrames into a PostgreSQL database in the correct order.

    This function retrieves database credentials from environment variables, creates a
    SQLAlchemy engine, and then iterates through a predefined load order to ensure
    relational integrity (parent tables before child tables).

    Args:
        dataframes: A dictionary of table names to their transformed DataFrames.
    """
    # 1. Retrieve DB credentials securely from environment variables
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    db_name = os.getenv("POSTGRES_DB")

    if not all([db_user, db_password, db_host, db_port, db_name]):
        print("Error: Database environment variables are not fully set.")
        return

    # 2. Create the database connection engine
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)

    # 3. Define load order to respect foreign key constraints
    # Parent tables must be loaded before their dependent child tables.
    LOAD_ORDER = [
        'customers',
        'geolocation',
        'sellers',
        'product_category_name_translation',
        'products',
        'orders', # Depends on 'customers'
        'order_items', # Depends on 'orders', 'products', 'sellers'
        'order_payments', # Depends on 'orders'
        'order_reviews' # Depends on 'orders'
    ]

    # 4. Load dataframes into SQL tables
    print("--- Starting Data Load ---")
    for table_name in LOAD_ORDER:
        if table_name in dataframes:
            df = dataframes[table_name]
            print(f"Loading {len(df)} rows into '{table_name}'...")
            try:
                # Using 'append' to avoid overwriting existing data.
                # 'chunksize' processes the DataFrame in batches to manage memory.
                df.to_sql(
                    table_name,
                    engine,
                    if_exists='append',
                    index=False,
                    chunksize=1000
                )
                print(f"✅ Success: '{table_name}' loaded.")
            except Exception as e:
                print(f"❌ Error loading data into '{table_name}': {e}")
                # Optional: break the loop if a critical table fails to load
                # break
        else:
            print(f"⚠️ Warning: DataFrame for table '{table_name}' not found in input.")
    print("--- Data Load Complete ---")