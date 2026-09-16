{% snapshot snap_accounts %}

{{
    config(
      target_database='BANKING_DB',
      target_schema='ANALYTICS_STAGING',
      unique_key='account_id',

      strategy='check',
      check_cols=['customer_id', 'balance', 'account_type'],
    )
}}

select * from {{ ref('stg_accounts') }}

{% endsnapshot %}