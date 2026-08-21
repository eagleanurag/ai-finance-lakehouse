CREATE OR REPLACE TABLE gold.kpi_monthly_actual_vs_budget AS
WITH actuals AS (
    SELECT
        date_trunc('month', dd.full_date)::DATE AS month_start,
        f.department_id,
        dep.dept_code,
        dep.department_name,
        SUM(CASE WHEN acc.category = 'Revenue' THEN f.amount_usd ELSE 0 END) AS revenue_actual,
        SUM(CASE WHEN acc.category <> 'Revenue' THEN f.amount_usd ELSE 0 END) AS expense_actual
    FROM gold.fact_transactions f
    JOIN gold.dim_date dd ON f.date_sk = dd.date_sk
    JOIN gold.dim_gl_account acc ON f.account_id = acc.account_id
    JOIN gold.dim_department dep ON f.department_id = dep.department_id
    GROUP BY 1, 2, 3, 4
),
budgets AS (
    SELECT
        b.month_start,
        b.department_id,
        SUM(b.budget_amount_usd) AS budget_expense
    FROM gold.fact_budget b
    GROUP BY 1, 2
)
SELECT
    act.month_start,
    CAST(strftime(act.month_start, '%Y%m') AS INTEGER) AS month_sk,
    act.department_id,
    act.dept_code,
    act.department_name,
    act.revenue_actual,
    act.expense_actual,
    COALESCE(bud.budget_expense, 0) AS budget_expense,
    ROUND(100.0 * (act.revenue_actual - act.expense_actual)
        / NULLIF(act.revenue_actual, 0), 2) AS net_margin_pct,
    ROUND(100.0 * (act.expense_actual - COALESCE(bud.budget_expense, 0))
        / NULLIF(bud.budget_expense, 0), 2) AS expense_variance_pct,
    ROUND(act.expense_actual / NULLIF(act.revenue_actual, 0), 4) AS burn_rate
FROM actuals act
LEFT JOIN budgets bud
    ON act.month_start = bud.month_start AND act.department_id = bud.department_id;
