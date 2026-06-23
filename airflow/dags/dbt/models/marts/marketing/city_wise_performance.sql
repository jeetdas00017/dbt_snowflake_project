-- ---------------------------------------------------------------------
-- marketing_channel_performance
-- Marketing Mart: acquisition channel performance — customers gained,
-- conversion rate, total/average revenue per channel.
-- ---------------------------------------------------------------------

with customers as (
    select customer_sk, customer_id, city , signup_date
    from {{ ref('dim_customers') }}
    where is_current = true
),

fact as (
    select * from {{ ref('fact_orders') }}
    where order_status not in ('cancelled', 'returned')
),

orders_by_customer as (
    select
        customer_sk,
        count(distinct order_id)  as total_orders,
        sum(total_amount)         as total_revenue
    from fact
    group by 1
)

select
    city,
    count(distinct c.customer_id)                                                  as total_customers,
    sum(coalesce(o.total_orders, 0))                                               as total_orders,
    sum(coalesce(o.total_revenue, 0))                                              as total_revenue,
    sum(coalesce(o.total_revenue, 0)) / nullif(count(distinct c.customer_id), 0)  as revenue_per_customer,
    sum(case when o.total_orders > 0 then 1 else 0 end)::float
        / nullif(count(distinct c.customer_id), 0)                                 as conversion_rate
from customers c
left join orders_by_customer o on c.customer_sk = o.customer_sk
group by 1
