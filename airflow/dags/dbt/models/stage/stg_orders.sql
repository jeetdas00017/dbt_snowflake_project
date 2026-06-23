{{
    config(
        materialized='incremental',
        unique_key='product_id',
        incremental_strategy='merge'
    )
}}


select
    order_id,
    customer_id,
    product_id,
    cast(order_date as date) as order_date,
    quantity,
    unit_price,
    coalesce(discount, 0)   as discount,
    total_amount,
    coalesce(trim(lower(order_status)), 'unknown')     as order_status,
    coalesce(trim(lower(payment_method)), 'unknown')   as payment_method,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from {{ source('raw', 'orders') }}

where order_id is not null
    and quantity >= 0
    and total_amount >= 0

{% if is_incremental() %}
  and (
    cast(updated_at as timestamp) > (select coalesce(max(cast(updated_at as timestamp)), '1900-01-01') from {{ this }})
    or order_id not in (select order_id from {{ this }})
  )
{% endif %}

