INSERT INTO analytics.dim_payment_methods (payment_method_name)
SELECT DISTINCT payment_method
FROM staging.marketplace
WHERE payment_method IS NOT NULL
ON CONFLICT (payment_method_name) DO NOTHING;