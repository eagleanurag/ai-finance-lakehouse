CREATE OR REPLACE TABLE gold.dim_vendor AS
WITH names AS (
    SELECT DISTINCT vendor_name FROM silver.silver_transactions WHERE vendor_name IS NOT NULL
    UNION
    SELECT DISTINCT vendor_name FROM raw.vendors_src WHERE vendor_name IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY n.vendor_name) AS vendor_id,
    n.vendor_name,
    COALESCE(m.vendor_category, 'Unclassified') AS vendor_category,
    COALESCE(m.country, 'Unknown') AS country
FROM names n
LEFT JOIN (
    SELECT vendor_name,
        arg_max(vendor_category, load_batch) AS vendor_category,
        arg_max(country, load_batch) AS country
    FROM raw.vendors_src
    GROUP BY vendor_name
) m USING (vendor_name);
