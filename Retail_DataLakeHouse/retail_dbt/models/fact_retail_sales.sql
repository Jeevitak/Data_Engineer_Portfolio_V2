-- models/fact_retail_sales.sql
WITH sales_data AS (
    SELECT
        sale_id,
        product_id,
        store_id,
        date,
        total_amount
    FROM {{ ref('clean_sales') }}
)
SELECT
    sale_id,
    product_id,
    store_id,
    date,
    total_amount,
    strftime('%Y', date) AS sale_year,
    strftime('%m', date) AS sale_month
FROM sales_data
