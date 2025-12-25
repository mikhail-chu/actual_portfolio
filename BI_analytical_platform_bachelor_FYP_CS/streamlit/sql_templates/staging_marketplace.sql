CREATE TABLE IF NOT EXISTS staging.marketplace (
    transaction_id INTEGER,
    date TIMESTAMP,
    product_id TEXT,
    category TEXT,
    product_name TEXT,
    units_sold INTEGER,
    price NUMERIC(10,2),
    total_revenue NUMERIC(12,2),
    region TEXT,
    payment_method TEXT
);