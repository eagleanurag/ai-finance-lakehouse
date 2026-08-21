CREATE SCHEMA IF NOT EXISTS silver;
CREATE OR REPLACE TABLE silver.silver_transactions AS
SELECT
    transaction_id,
    TRY_CAST(event_ts AS TIMESTAMPTZ) AS event_ts,
    CAST(event_date AS DATE) AS event_date,
    UPPER(dept_code) AS dept_code,
    account_code,
    vendor_name,
    CAST(amount_usd AS DECIMAL(18, 2)) AS amount_usd,
    currency,
    txn_type,
    source_channel,
    load_batch
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY load_batch DESC
        ) AS rn
    FROM raw.transactions
    WHERE transaction_id IS NOT NULL
)
WHERE rn = 1;
