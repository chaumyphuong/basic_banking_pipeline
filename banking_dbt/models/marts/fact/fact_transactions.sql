{{ config(materialized='table') }}

select
    t.transaction_id,
    a.account_sk,
    c.customer_sk,
    t.account_id,
    a.customer_id,
    t.related_account_id,
    t.transaction_type,
    t.status,
    t.amount,
    t.transaction_date,
    t.loaded_at
from {{ ref('stg_transactions') }} t
left join {{ ref('dim_accounts') }} a 
    on t.account_id = a.account_id 
left join {{ ref('dim_customers') }} c 
    on a.customer_id = c.customer_id 
