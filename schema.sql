-- Geolocations
CREATE TABLE geolocations (
    geolocation_zip_code_prefix VARCHAR(10) PRIMARY KEY,
    geolocation_lat NUMERIC(9,6) NOT NULL,
    geolocation_lng NUMERIC(9,6) NOT NULL,
    geolocation_city VARCHAR(100) NOT NULL,
    geolocation_state CHAR(2) NOT NULL
);

-- Customers
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    customer_unique_id UUID NOT NULL,
    customer_zip_code_prefix VARCHAR(10) NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state CHAR(2) NOT NULL,
    CONSTRAINT fk_customer_geolocation
        FOREIGN KEY (customer_zip_code_prefix)
        REFERENCES geolocations (geolocation_zip_code_prefix)
);
CREATE INDEX idx_customers_zip_prefix ON customers(customer_zip_code_prefix);

-- Sellers
CREATE TABLE sellers (
    seller_id UUID PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10) NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state CHAR(2) NOT NULL,
    CONSTRAINT fk_seller_geolocation
        FOREIGN KEY (seller_zip_code_prefix)
        REFERENCES geolocations (geolocation_zip_code_prefix)
);
CREATE INDEX idx_sellers_zip_prefix ON sellers(seller_zip_code_prefix);

-- Products
CREATE TABLE products (
    product_id UUID PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT CHECK (product_name_length >= 0),
    product_description_length INT CHECK (product_description_length >= 0),
    product_photos_qty INT CHECK (product_photos_qty >= 0),
    product_weight_g INT CHECK (product_weight_g >= 0),
    product_length_cm INT CHECK (product_length_cm >= 0),
    product_height_cm INT CHECK (product_height_cm >= 0),
    product_width_cm INT CHECK (product_width_cm >= 0)
);

-- Product category translation
CREATE TABLE product_category_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100) NOT NULL
);

-- Orders
CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP NOT NULL,
    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers (customer_id)
);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(order_status);

-- Order items
CREATE TABLE order_items (
    order_id UUID NOT NULL,
    order_item_id INT NOT NULL,
    product_id UUID NOT NULL,
    seller_id UUID NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    freight_value NUMERIC(10,2) NOT NULL CHECK (freight_value >= 0),
    PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_item_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),
    CONSTRAINT fk_item_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id),
    CONSTRAINT fk_item_seller
        FOREIGN KEY (seller_id)
        REFERENCES sellers (seller_id)
);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_order_items_seller_id ON order_items(seller_id);

-- Order payments
CREATE TABLE order_payments (
    order_id UUID NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments INT NOT NULL CHECK (payment_installments >= 0),
    payment_value NUMERIC(10,2) NOT NULL CHECK (payment_value >= 0),
    PRIMARY KEY (order_id, payment_sequential),
    CONSTRAINT fk_payment_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id)
);
CREATE INDEX idx_order_payments_order_id ON order_payments(order_id);
CREATE INDEX idx_order_payments_type ON order_payments(payment_type);

-- Order reviews
CREATE TABLE order_reviews (
    review_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    review_score INT CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP,
    CONSTRAINT fk_review_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id)
);
CREATE INDEX idx_order_reviews_order_id ON order_reviews(order_id);
CREATE INDEX idx_order_reviews_score ON order_reviews(review_score);
