-- Reporting on one definition of sales across all regions

-- 1. Global sales by region and month, in USD and INR
SELECT d.year_month, f.region,
       SUM(f.amount_usd) AS sales_usd,
       SUM(f.amount_inr) AS sales_inr,
       SUM(f.quantity)   AS units
FROM dbo.fact_sales f
JOIN dbo.dim_date d ON d.date_key = f.date_key
GROUP BY d.year_month, f.region
ORDER BY d.year_month, f.region;

-- 2. Operations: average fulfilment time by region and store format
SELECT f.region, s.[format],
       AVG(CAST(f.fulfilment_days AS DECIMAL(6,2))) AS avg_fulfilment_days,
       COUNT(*) AS order_lines
FROM dbo.fact_sales f
JOIN dbo.dim_store s ON s.store_key = f.store_key
GROUP BY f.region, s.[format]
ORDER BY f.region, avg_fulfilment_days;

-- 3. Category mix per region (share of USD sales)
SELECT f.region, p.category,
       SUM(f.amount_usd) * 100.0 / SUM(SUM(f.amount_usd)) OVER (PARTITION BY f.region) AS pct_of_region
FROM dbo.fact_sales f
JOIN dbo.dim_product p ON p.product_key = f.product_key
GROUP BY f.region, p.category
ORDER BY f.region, pct_of_region DESC;

-- 4. Before vs after a store format change (SCD Type 2)
SELECT s.store_code, s.[format], s.valid_from, s.valid_to, SUM(f.amount_usd) AS sales_usd
FROM dbo.fact_sales f
JOIN dbo.dim_store s ON s.store_key = f.store_key
WHERE s.store_code = 'IT-01'
GROUP BY s.store_code, s.[format], s.valid_from, s.valid_to;
