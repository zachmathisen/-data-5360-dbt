{{ config(
    materialized = 'table',
    schema = 'ecoessentials'
) }}

SELECT
  product_id        AS prod_key,
  product_id        AS product_id,
  product_type      AS product_type,
  product_name      AS product_name,
  price             AS price

FROM {{ source('online_purchases', 'product') }}
