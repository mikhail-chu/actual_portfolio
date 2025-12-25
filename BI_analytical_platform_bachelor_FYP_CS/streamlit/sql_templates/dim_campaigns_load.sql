INSERT INTO analytics.dim_campaigns (campaign_id, campaign_name, category_id)
SELECT DISTINCT
    a.campaign_id,
    a.name,
    c.category_id
FROM staging.advertising a
JOIN analytics.dim_categories c
    ON c.category_name = a.category
ON CONFLICT (campaign_id) DO NOTHING;