SELECT
    c.campaign_id,
    c.campaign_name,
    cat.category_name,
    f.start_date,
    f.end_date,
    f.duration_days,
    f.total_budget,
    COALESCE(SUM(r.units_sold * r.price), 0) AS revenue_during_campaign,
    CASE
        WHEN f.total_budget > 0 THEN
            COALESCE(SUM(r.units_sold * r.price), 0) / f.total_budget
        ELSE NULL
    END AS roi
FROM analytics.fact_advertising_campaigns f
JOIN analytics.dim_campaigns c
    ON c.campaign_id = f.campaign_id
JOIN analytics.dim_categories cat
    ON cat.category_id = c.category_id
LEFT JOIN analytics.fact_retail_sales r
    ON r.date BETWEEN f.start_date AND f.end_date
GROUP BY
    c.campaign_id,
    c.campaign_name,
    cat.category_name,
    f.start_date,
    f.end_date,
    f.duration_days,
    f.total_budget
ORDER BY roi DESC;