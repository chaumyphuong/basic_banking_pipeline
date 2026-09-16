{{ config(materialized='table') }}

select
    dbt_scd_id                                        as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    dbt_valid_from                                    as valid_from,
    dbt_valid_to                                      as valid_to,
    case 
        when dbt_valid_to is null then true 
        else false 
    end                                               as is_current

from {{ ref('snap_customers') }}