{% snapshot snap_customers %}

{{
    config(
      target_database='BANKING_DB',
      target_schema='ANALYTICS_STAGING',
      unique_key='customer_id',

      strategy='check',
      check_cols=['email', 'first_name', 'last_name'],
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}