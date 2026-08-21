CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.dim_date AS
SELECT
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_sk,
    full_date,
    year(full_date) AS year_no,
    quarter(full_date) AS quarter_no,
    month(full_date) AS month_no,
    strftime(full_date, '%B') AS month_name,
    CAST(strftime(full_date, '%Y-%m') AS VARCHAR) AS year_month,
    weekofyear(full_date) AS week_no,
    dayofweek(full_date) AS day_of_week,
    (dayofweek(full_date) IN (0, 6)) AS is_weekend,
    (full_date = last_day(full_date)) AS is_month_end
FROM (
    SELECT unnest(
        generate_series(
            (SELECT min(event_date) FROM silver.silver_transactions),
            (SELECT max(event_date) FROM silver.silver_transactions),
            INTERVAL 1 DAY
        )
    )::DATE AS full_date
);
