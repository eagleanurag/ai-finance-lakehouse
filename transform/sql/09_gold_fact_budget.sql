CREATE OR REPLACE TABLE gold.fact_budget AS
WITH budget_monthly AS (
    SELECT
        b.month_start,
        b.dept_code,
        b.account_code,
        SUM(b.budget_amount_usd) AS budget_amount_usd
    FROM silver.silver_budgets b
    GROUP BY 1, 2, 3
)
SELECT
    ROW_NUMBER() OVER () AS budget_sk,
    CAST(strftime(b.month_start, '%Y%m%d') AS INTEGER) AS date_sk,
    b.month_start,
    d.department_id,
    a.account_id,
    b.budget_amount_usd
FROM budget_monthly b
JOIN gold.dim_department d ON b.dept_code = d.dept_code
JOIN gold.dim_gl_account a ON b.account_code = a.account_code;
