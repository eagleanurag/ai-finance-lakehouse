CREATE OR REPLACE TABLE gold.dim_gl_account AS
WITH codes AS (
    SELECT DISTINCT account_code FROM silver.silver_transactions
    UNION
    SELECT DISTINCT account_code FROM silver.silver_budgets
    UNION
    SELECT DISTINCT account_code FROM raw.gl_accounts_src
)
SELECT
    ROW_NUMBER() OVER (ORDER BY c.account_code) AS account_id,
    c.account_code,
    COALESCE(m.account_name, 'Unknown ' || c.account_code) AS account_name,
    COALESCE(m.category, 'Unclassified') AS category
FROM codes c
LEFT JOIN (
    SELECT account_code,
        arg_max(account_name, load_batch) AS account_name,
        arg_max(category, load_batch) AS category
    FROM raw.gl_accounts_src
    GROUP BY account_code
) m USING (account_code);
