CREATE OR REPLACE TABLE silver.silver_budgets AS
SELECT
    budget_id,
    strptime(year_month || '-01', '%Y-%m-%d')::DATE AS month_start,
    UPPER(dept_code) AS dept_code,
    account_code,
    CAST(budget_amount_usd AS DECIMAL(18, 2)) AS budget_amount_usd,
    source_channel,
    load_batch
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(budget_id, year_month || dept_code || account_code)
            ORDER BY load_batch DESC
        ) AS rn
    FROM raw.budgets
)
WHERE rn = 1;
