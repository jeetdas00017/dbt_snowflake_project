-- ---------------------------------------------------------------------
-- sales_customer_ltv
-- Sales Mart: per-customer lifetime value, order frequency, and
-- average order value (used for sales rep account prioritization).
-- ---------------------------------------------------------------------

with fact as (
    select * from {{ ref('fact_orders') }}
    where order_status not in ('cancelled', 'returned')
),

customers as (
    select customer_sk, customer_id, first_name, last_name, email, country
    from {{ ref('dim_customers') }}
    where is_current = true
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    count(distinct f.order_id)                                     as total_orders,
    sum(f.total_amount)                                            as lifetime_value,
    min(f.order_date)                                              as first_order_date,
    max(f.order_date)                                              as last_order_date,
    sum(f.total_amount) / nullif(count(distinct f.order_id), 0)    as avg_order_value
from fact f
join customers c on f.customer_sk = c.customer_sk
group by 1, 2, 3, 4, 5
