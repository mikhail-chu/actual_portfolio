INSERT INTO analytics.dim_categories (category_name)
SELECT DISTINCT category
FROM staging.retail
WHERE category IS NOT NULL
ON CONFLICT (category_name) DO NOTHING;