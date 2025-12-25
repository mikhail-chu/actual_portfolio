INSERT INTO analytics.dim_stores (store_id, region_id)
SELECT DISTINCT
    r.store_id,
    rg.region_id
FROM staging.retail r
JOIN analytics.dim_regions rg
    ON rg.region_name = r.region
ON CONFLICT (store_id) DO NOTHING;