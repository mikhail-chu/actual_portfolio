INSERT INTO analytics.dim_products (product_id, product_name, category_id)
SELECT DISTINCT
    m.product_id,
    m.product_name,
    c.category_id
FROM staging.marketplace m
JOIN analytics.dim_categories c
    ON c.category_name = m.category
ON CONFLICT (product_id) DO NOTHING;