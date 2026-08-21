CREATE OR REPLACE TABLE silver.silver_invoices AS
SELECT
    invoice_number,
    TRY_CAST(invoice_date AS DATE) AS invoice_date,
    vendor_name_guess AS vendor_name,
    CAST(total_amount_usd AS DECIMAL(18, 2)) AS total_amount_usd,
    CAST(tax_amount_usd AS DECIMAL(18, 2)) AS tax_amount_usd,
    ocr_engine,
    source_file,
    load_batch
FROM raw.invoices;
