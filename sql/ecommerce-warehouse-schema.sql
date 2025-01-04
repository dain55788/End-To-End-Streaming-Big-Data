-- Dimension Tables

CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    base_price DECIMAL(10,2),
    supplier VARCHAR(100),
    is_active TINYINT(1),
    valid_from TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(255),
    segment VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    is_weekend TINYINT(1),
    is_holiday TINYINT(1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    timezone VARCHAR(50),
    region VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_payment (
    payment_id INT PRIMARY KEY,
    method VARCHAR(50),
    status VARCHAR(50),
    provider VARCHAR(100),
    processing_fee DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_channel (
    channel_id INT PRIMARY KEY,
    channel_name VARCHAR(100),
    channel_type VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Tables

CREATE TABLE fact_sales (
    sale_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    customer_id INT,
    date_id INT,
    location_id INT,
    payment_id INT,
    amount DECIMAL(10,2),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    tax DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id)
);

CREATE TABLE fact_product_reviews (
    review_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    date_id INT,
    rating INT,
    review_text TEXT,
    helpful_votes INT DEFAULT 0,
    is_verified_purchase TINYINT(1) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_customer_interactions (
    interaction_id INT PRIMARY KEY,
    customer_id INT,
    date_id INT,
    channel_id INT,
    interaction_type VARCHAR(50),
    interaction_details TEXT,
    resolution_status VARCHAR(50),
    duration_seconds INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id)
);

-- Indexes for better query performance

CREATE INDEX idx_fact_sales_dates ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);

CREATE INDEX idx_fact_reviews_product ON fact_product_reviews(product_id);
CREATE INDEX idx_fact_reviews_customer ON fact_product_reviews(customer_id);
CREATE INDEX idx_fact_reviews_date ON fact_product_reviews(date_id);

CREATE INDEX idx_fact_interactions_customer ON fact_customer_interactions(customer_id);
CREATE INDEX idx_fact_interactions_date ON fact_customer_interactions(date_id);
CREATE INDEX idx_fact_interactions_channel ON fact_customer_interactions(channel_id);