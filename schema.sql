-- Geolocations
CREATE TABLE geolocations (
    zip_code_prefix VARCHAR(10) PRIMARY KEY,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    state CHAR(2) NOT NULL
);

-- Customers
CREATE TABLE customers (
    id UUID PRIMARY KEY,
    unique_id UUID NOT NULL,
    zip_code_prefix VARCHAR(10) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state CHAR(2) NOT NULL
);
CREATE INDEX idx_customers_zip_prefix ON customers(zip_code_prefix);

-- Sellers
CREATE TABLE sellers (
    id UUID PRIMARY KEY,
    zip_code_prefix VARCHAR(10) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state CHAR(2) NOT NULL
);
CREATE INDEX idx_sellers_zip_prefix ON sellers(zip_code_prefix);

-- Products
CREATE TABLE products (
    id UUID PRIMARY KEY,
    category VARCHAR(100),
    name_length INT,
    description_length INT,
    photos_qty INT,
    weight_g INT,
    length_cm INT,
    height_cm INT,
    width_cm INT,
    volume_cm3 NUMERIC(15, 2)
);

-- Product category translation
CREATE TABLE product_category_translation (
    category VARCHAR(100) PRIMARY KEY,
    category_english VARCHAR(100) NOT NULL
);

-- Orders
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer UUID NOT NULL,
    status VARCHAR(50) NOT NULL,
    purchase TIMESTAMP NOT NULL,
    approved TIMESTAMP,
    carrier_delivery TIMESTAMP,
    customer_delivery TIMESTAMP,
    estimated_delivery TIMESTAMP NOT NULL,
    delivery_time_days INT,
    approval_time_days INT,
    delivery_lateness_days INT,
    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer)
        REFERENCES customers (id)
);
CREATE INDEX idx_orders_customer_id ON orders(customer);

-- Order items
CREATE TABLE order_items (
    id UUID NOT NULL,
    item INT NOT NULL,
    product UUID NOT NULL,
    seller UUID NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    freight NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (id, item),
    CONSTRAINT fk_item_order
        FOREIGN KEY (id)
        REFERENCES orders (id),
    CONSTRAINT fk_item_product
        FOREIGN KEY (product)
        REFERENCES products (id),
    CONSTRAINT fk_item_seller
        FOREIGN KEY (seller)
        REFERENCES sellers (id)
);
CREATE INDEX idx_order_items_product_id ON order_items(product);
CREATE INDEX idx_order_items_seller_id ON order_items(seller);

-- Order payments
CREATE TABLE order_payments (
    id UUID PRIMARY KEY, -- Corresponds to order_id
    total_paid NUMERIC(10, 2),
    num_payments INT,
    payment_chunk_count INT,
    payment_type_mode VARCHAR(50),
    CONSTRAINT fk_payment_order
        FOREIGN KEY (id)
        REFERENCES orders (id)
);

-- Order reviews
CREATE TABLE order_reviews (
    id UUID PRIMARY KEY, -- Corresponds to order_id
    num_reviews INT,
    score_avg NUMERIC(3, 2),
    review_date_latest TIMESTAMP,
    answer_date_latest TIMESTAMP,
    CONSTRAINT fk_review_order
        FOREIGN KEY (id)
        REFERENCES orders (id)
);