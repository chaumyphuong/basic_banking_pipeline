{{ config(materialized='view') }}

with source_data as (
    select
        V:id::integer                      as transaction_id,
        V:account_id::integer              as account_id,
        V:amount::numeric(18, 2)           as amount,
        V:txn_type::string                 as transaction_type,
        V:status::string                   as status,
        V:related_account::integer         as related_account_id,
        V:created_at::timestamp_tz         as transaction_date,
        LOADED_AT,
        row_number() over (
            partition by V:id::integer
            order by V:created_at::timestamp_tz desc, LOADED_AT desc
        ) as rn
    from {{ source('raw_banking', 'transactions') }}
)

select
    transaction_id,
    account_id,
    amount,
    transaction_type,
    status,
    related_account_id,
    transaction_date,
    LOADED_AT
from source_data
where rn = 1