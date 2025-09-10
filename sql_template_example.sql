DROP TABLE IF EXISTS joined_table;
CREATE TEMPORARY TABLE joined_table(
  product_id INTEGER,
  ga_sku VARCHAR(100),
  "date" DATE NOT NULL,
  shop_name VARCHAR(80),
  revenue_in_purchase_price MONEY,
  revenue_in_shop_price MONEY,
  amount NUMERIC,
  consultant_name VARCHAR(200),
	country VARCHAR(50)
) ON COMMIT DROP;

WITH rd(date, ga_sku, consultant_name, shop_name, revenue_in_purchase_price, revenue_in_shop_price, amount, country) as ( VALUES  
%s
)

INSERT INTO joined_table
SELECT  ap.product_id, rd.ga_sku, rd.date::DATE, rd.shop_name,
		rd.revenue_in_purchase_price, rd.revenue_in_shop_price,
		rd.amount, rd.consultant_name, rd.country
FROM rd
LEFT JOIN (SELECT ap.id AS product_id,
		   pchs.sku AS ga_sku
	FROM pusy.products_channels_sku_m2m AS pchs
	JOIN pusy.products AS pr
	ON (pchs.product_id = pr.id)
	JOIN cross_project.all_products AS ap
		ON (ap.id = pr.cross_project_id)
	WHERE channel_id = 8
	UNION
	SELECT ap.id AS product_id,
		   pchs.sku AS ga_sku
	FROM yesbaby.products_channels_sku_m2m AS pchs
	JOIN yesbaby.products AS pr
	ON (pchs.product_id = pr.id)
	JOIN cross_project.all_products AS ap
		ON (ap.id = pr.cross_project_id)
	WHERE channel_id = 8
	) AS ap
USING (ga_sku);


INSERT INTO cross_project.golden_apple_sales
SELECT product_id, date::DATE, shop_name,
		sum(revenue_in_purchase_price) AS revenue_in_purchase_price,
    sum(revenue_in_shop_price) AS revenue_in_shop_price,
		sum(amount) AS amount,
    consultant_name, country
FROM joined_table
WHERE product_id IS NOT NULL
GROUP BY (product_id, "date", shop_name, consultant_name, country)
ON CONFLICT (product_id, "date", shop_name, consultant_name, country) DO UPDATE
	SET revenue_in_purchase_price = excluded.revenue_in_purchase_price,
			revenue_in_shop_price = excluded.revenue_in_shop_price,
			amount = excluded.amount;

SELECT * FROM system.log_rows('upsert_ga_sales',
'SELECT product_id, ga_sku , "date", shop_name,
        revenue_in_purchase_price::numeric,
        revenue_in_shop_price::numeric,
        amount, consultant_name, country
FROM joined_table
WHERE product_id IS NULL;');