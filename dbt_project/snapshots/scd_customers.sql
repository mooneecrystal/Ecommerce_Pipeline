{% snapshot scd_customers %}

{{
    config(
      target_schema='silver',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='load_date'
    )
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}