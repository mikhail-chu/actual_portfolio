CREATE TABLE IF NOT EXISTS staging.retail (
    date DATE,
    store_id TEXT,
    product_id TEXT,
    category TEXT,
    region TEXT,
    inventory_level NUMERIC,
    units_sold NUMERIC,
    units_ordered NUMERIC,
    demand_forecast NUMERIC,
    price NUMERIC(10,2),
    discount NUMERIC(5,2),
    weather_condition TEXT,
    holiday_promotion TEXT,
    competitor_pricing NUMERIC(10,2),
    seasonality TEXT
);