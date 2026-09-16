{{ config(materialized='table') }}

select
    dbt_scd_id                                        as account_sk,
    account_id,
    customer_id,
    account_type,
    balance,
    created_at,
    dbt_valid_from                                    as valid_from,
    dbt_valid_to                                      as valid_to,
    case 
        when dbt_valid_to is null then true 
        else false 
    end                                               as is_current

from {{ ref('snap_accounts') }}