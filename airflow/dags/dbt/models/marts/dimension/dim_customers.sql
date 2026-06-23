-- ---------------------------------------------------------------------
-- dim_customers
-- Reads the SCD2 snapshot (full history) and exposes a clean dimension
-- with a surrogate key, validity window, and an `is_current` flag.
-- ---------------------------------------------------------------------


select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    country,
    signup_date,
    dbt_valid_from                                       as effective_from,
    coalesce(dbt_valid_to, '9999-12-31'::timestamp)      as effective_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_customers') }}
