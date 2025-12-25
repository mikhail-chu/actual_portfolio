INSERT INTO analytics.fact_marketplace_sales
SELECT
    m.transaction_id,
    m.date,
    m.product_id,
    c.category_id,
    rg.region_id,
    pm.payment_method_id,
    m.units_sold,
    m.price,
    m.total_revenue
FROM staging.marketplace m
JOIN analytics.dim_categories c
    ON c.category_name = m.category
JOIN analytics.dim_regions rg
    ON rg.region_name = m.region
JOIN analytics.dim_payment_methods pm
    ON pm.payment_method_name = m.payment_method
ON CONFLICT (transaction_id) DO NOTHING;