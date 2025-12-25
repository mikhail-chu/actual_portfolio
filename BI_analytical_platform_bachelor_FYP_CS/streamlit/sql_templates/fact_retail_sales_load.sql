INSERT INTO analytics.fact_retail_sales (
    date,
    store_id,
    product_id,
    category_id,
    region_id,
    inventory_level,
    units_sold,
    units_ordered,
    demand_forecast,
    price,
    discount,
    competitor_pricing,
    weather_condition,
    holiday_promotion,
    seasonality
)
SELECT
    r.date,
    r.store_id,
    r.product_id,
    c.category_id,
    rg.region_id,
    r.inventory_level,
    r.units_sold,
    r.units_ordered,
    r.demand_forecast,
    r.price,
    r.discount,
    r.competitor_pricing,
    r.weather_condition,
    CASE
        WHEN r.holiday_promotion = 1 THEN TRUE
        ELSE FALSE
    END AS holiday_promotion,
    r.seasonality
FROM staging.retail r
JOIN analytics.dim_categories c
    ON c.category_name = r.category
JOIN analytics.dim_regions rg
    ON rg.region_name = r.region
ON CONFLICT (date, store_id, product_id) DO NOTHING;