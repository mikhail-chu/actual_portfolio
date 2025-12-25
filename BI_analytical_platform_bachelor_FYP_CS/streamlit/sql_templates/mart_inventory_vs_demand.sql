SELECT
    r.date,
    r.store_id,
    r.product_id,
    c.category_name,
    rg.region_name,
    r.inventory_level,
    r.demand_forecast,
    r.units_sold,
    r.inventory_level - r.demand_forecast AS inventory_gap
FROM analytics.fact_retail_sales r
JOIN analytics.dim_categories c
    ON c.category_id = r.category_id
JOIN analytics.dim_regions rg
    ON rg.region_id = r.region_id
ORDER BY r.date, r.store_id, r.product_id;