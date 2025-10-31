{{ config(materialized='table', schema='ecoessentials', alias='dim_order') }}

with order_lines as (
  select
    order_line_id,
    order_id,
    quantity,
    discount,
    price_after_discount
  from {{ source('online_purchases','order_line') }}
),
orders as (
  select
    order_id,
    -- adjust to your actual column name; alias to a consistent name
    order_timestamp as order_timestamp
  from {{ source('online_purchases','order') }}
)

select
  ol.order_line_id        as order_line_key,
  ol.order_id             as order_id,
  ol.quantity             as quantity,
  ol.discount             as discount,
  ol.price_after_discount as price_after_discount,
  o.order_timestamp       as order_timestamp
from order_lines ol
left join orders o
  on o.order_id = ol.order_id
