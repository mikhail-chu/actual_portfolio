CREATE TABLE IF NOT EXISTS staging.advertising (
    campaign_id INTEGER,
    name TEXT,
    category TEXT,
    start_date DATE,
    end_date DATE,
    duration_days INTEGER,
    total_budget NUMERIC(12,2)
);