-- ---------------------------------------------------------------------
-- sales_daily_summary
-- Sales Mart: daily revenue, units, and order counts sliced by
-- product category/brand and customer geography.
-- ---------------------------------------------------------------------

with fact as (
    select * from {{ ref('fact_orders') }}
    where order_status not in ('cancelled')
),

products as (
    select product_sk, category, sub_category, brand
    from {{ ref('dim_products') }}
    where is_current = true
),

customers as (
    select customer_sk, country
    from {{ ref('dim_customers') }}
    where is_current = true
)

select
    cast(f.order_date as date)    as order_date,
    p.category,
    p.sub_category,
    p.brand,
    c.country,
    sum(f.quantity)                as total_units_sold,
    sum(f.total_amount)            as total_revenue,
    sum(f.discount * f.quantity)   as total_discount,
    count(distinct f.order_id)     as total_orders,
    count(distinct f.customer_id)  as unique_customers
from fact f
left join products  p on f.product_sk  = p.product_sk
left join customers c on f.customer_sk = c.customer_sk
group by 1, 2, 3, 4, 5
