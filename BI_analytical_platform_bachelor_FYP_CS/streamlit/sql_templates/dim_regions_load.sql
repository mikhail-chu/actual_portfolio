INSERT INTO analytics.dim_regions (region_name)
SELECT DISTINCT TRIM(region)
FROM (
    SELECT region FROM staging.marketplace
    UNION
    SELECT region FROM staging.retail
) s
LEFT JOIN analytics.dim_regions r
    ON TRIM(LOWER(r.region_name)) = TRIM(LOWER(s.region))
WHERE r.region_id IS NULL;