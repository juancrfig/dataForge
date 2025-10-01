from typing import Any


TABLES_CONFIG: list[dict[str, Any]] = [
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