SELECT
    r.date::date AS date,
    'retail' AS channel,
    r.units_sold,
    (r.units_sold * r.price) AS revenue
FROM analytics.fact_retail_sales r

UNION ALL

SELECT
    m.date::date AS date,
    'marketplace' AS channel,
    m.units_sold,
    m.total_revenue AS revenue
FROM analytics.fact_marketplace_sales m

ORDER BY date, channel;