select
    sale_id,
    date,
    store_id,
    product_id,
    quantity,
    unit_price,
    total_amount
from {{ ref('stg_sales') }}


