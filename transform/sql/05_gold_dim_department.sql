CREATE OR REPLACE TABLE gold.dim_department AS
WITH codes AS (
    SELECT DISTINCT dept_code FROM silver.silver_transactions
    UNION
    SELECT DISTINCT dept_code FROM silver.silver_budgets
    UNION
    SELECT DISTINCT dept_code FROM raw.departments_src WHERE dept_code IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY c.dept_code) AS department_id,
    c.dept_code,
    COALESCE(m.dept_name, 'Unknown ' || c.dept_code) AS department_name,
    COALESCE(m.cost_center, 'CC-NA') AS cost_center,
    CAST(COALESCE(m.headcount, 0) AS INTEGER) AS headcount
FROM codes c
LEFT JOIN (
    SELECT dept_code,
        arg_max(dept_name, load_batch) AS dept_name,
        arg_max(cost_center, load_batch) AS cost_center,
        arg_max(headcount, load_batch) AS headcount
    FROM raw.departments_src
    GROUP BY dept_code
) m USING (dept_code);
