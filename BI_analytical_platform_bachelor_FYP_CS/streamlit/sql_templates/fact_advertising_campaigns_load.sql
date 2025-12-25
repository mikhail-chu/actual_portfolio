INSERT INTO analytics.fact_advertising_campaigns
SELECT
    campaign_id,
    start_date,
    end_date,
    duration_days,
    total_budget
FROM staging.advertising;