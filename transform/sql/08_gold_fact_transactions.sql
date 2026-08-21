CREATE OR REPLACE TABLE gold.fact_transactions AS
SELECT
    ROW_NUMBER() OVER () AS transaction_sk,
    t.transaction_id,
    CAST(strftime(t.event_date, '%Y%m%d') AS INTEGER) AS date_sk,
    d.department_id,
    a.account_id,
    v.vendor_id,
    t.amount_usd,
    t.currency,
    t.txn_type,
    t.source_channel,
    t.event_ts
FROM silver.silver_transactions t
JOIN gold.dim_department d ON t.dept_code = d.dept_code
JOIN gold.dim_gl_account a ON t.account_code = a.account_code
LEFT JOIN gold.dim_vendor v ON t.vendor_name = v.vendor_name;
