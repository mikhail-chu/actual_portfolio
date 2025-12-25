SELECT
    r.date::date AS date,
    c.category_name,
    rg.region_name,
    r.units_sold,
    (r.units_sold * r.price) AS revenue
FROM analytics.fact_retail_sales r
JOIN analytics.dim_categories c
    ON c.category_id = r.category_id
JOIN analytics.dim_regions rg
    ON rg.region_id = r.region_id

ORDER BY date, category_name, region_name;