{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}



    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(email) as email,
        phone,
        city,
        country,
        cast(signup_date as date) as signup_date,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from {{ source('raw', 'customers') }}

    {% if is_incremental() %}
    where cast(updated_at as timestamp) > (select coalesce(max(cast(updated_at as timestamp)), '1900-01-01') from {{ this }})
    or customer_id not in (select customer_id from {{ this }})
    {% endif %}


