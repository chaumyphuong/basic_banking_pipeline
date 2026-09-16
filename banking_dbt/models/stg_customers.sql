{{ config(materialized='view') }}

with source_data as (
    select
        V:id::integer                 as customer_id,
        V:first_name::string          as first_name,
        V:lastname::string            as last_name,
        V:email::string               as email,
        V:created_at::timestamp_tz    as created_at,
        LOADED_AT,
        row_number() over (
            partition by V:id::integer
            order by V:created_at::timestamp_tz desc, LOADED_AT desc
        ) as rn
    from {{ source('raw_banking', 'customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    LOADED_AT
from source_data
where rn = 1
