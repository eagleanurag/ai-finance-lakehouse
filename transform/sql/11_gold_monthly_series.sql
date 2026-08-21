CREATE OR REPLACE TABLE gold.monthly_financials AS
SELECT
    month_start,
    CAST(strftime(month_start, '%Y%m') AS INTEGER) AS month_sk,
    SUM(revenue_actual) AS total_revenue,
    SUM(expense_actual) AS total_expense,
    SUM(revenue_actual) - SUM(expense_actual) AS net_profit,
    ROUND(100.0 * (SUM(revenue_actual) - SUM(expense_actual))
        / NULLIF(SUM(revenue_actual), 0), 2) AS net_margin_pct,
    ROUND(100.0 * (SUM(expense_actual) / NULLIF(SUM(budget_expense), 0) - 1) * 100.0, 2) AS expense_vs_budget_pct
FROM gold.kpi_monthly_actual_vs_budget
GROUP BY month_start;
