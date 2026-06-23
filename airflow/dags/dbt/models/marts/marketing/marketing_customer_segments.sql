-- ---------------------------------------------------------------------
-- marketing_customer_segments
-- Marketing Mart: RFM (Recency, Frequency, Monetary) segmentation,
-- used for campaign targeting and lifecycle marketing.
-- ---------------------------------------------------------------------

with fact as (
    select * from {{ ref('fact_orders') }}
    where order_status not in ('cancelled', 'returned')
),

customers as (
    select customer_sk, customer_id, email, signup_date
    from {{ ref('dim_customers') }}
    where is_current = true
),

customer_orders as (
    select
        customer_sk,
        max(order_date)             as last_order_date,
        count(distinct order_id)    as frequency,
        sum(total_amount)           as monetary
    from fact
    group by 1
),

scored as (
    select
        co.*,
        datediff(day, co.last_order_date, getdate()) as recency_days,
        ntile(5) over (order by datediff(day, co.last_order_date, getdate()) desc) as recency_score,
        ntile(5) over (order by co.frequency asc)                                   as frequency_score,
        ntile(5) over (order by co.monetary asc)                                    as monetary_score
    from customer_orders co
)

select
    c.customer_id,
    c.email,
    c.signup_date,
    s.last_order_date,
    s.recency_days,
    s.frequency,
    s.monetary,
    s.recency_score,
    s.frequency_score,
    s.monetary_score,
    (s.recency_score + s.frequency_score + s.monetary_score) as rfm_total_score,
    case
        when s.recency_score >= 4 and s.frequency_score >= 4 and s.monetary_score >= 4 then 'Champion'
        when s.recency_score >= 3 and s.frequency_score >= 3 then 'Loyal'
        when s.recency_score <= 2 and s.frequency_score <= 2 then 'At Risk'
        else 'Needs Attention'
    end as customer_segment
from scored s
join customers c on s.customer_sk = c.customer_sk
