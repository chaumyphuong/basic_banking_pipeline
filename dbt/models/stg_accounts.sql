{{ config(materialized='view') }}

with source_data as (
    select
        V:id::integer                  as account_id,
        V:customer_id::integer         as customer_id,
        V:account_type::string         as account_type,
        V:balance::numeric(18, 2)      as balance,
        V:currency::string             as currency,
        V:created_at::timestamp_tz     as created_at,
        LOADED_AT,
        row_number() over (
            partition by V:id::integer
            order by V:created_at::timestamp_tz desc, LOADED_AT desc
        ) as rn
    from {{ source('raw_banking', 'accounts') }}
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    LOADED_AT
from source_data
where rn = 1