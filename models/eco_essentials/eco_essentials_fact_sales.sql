{{ config(materialized='table', schema='ecoessentials', alias='fact_sales') }}

with ol as (
  select order_line_id, order_id, product_id, campaign_id, quantity, discount, price_after_discount
  from {{ source('online_purchases','order_line') }}
),
o as (
  select order_id, customer_id, order_timestamp
  from {{ source('online_purchases','order') }}
)

select
  do.order_line_key                 as order_line_key,
  dp.prod_key                    as prod_key,
  du.user_key                       as user_key,          -- may be NULL for marketing-only users
  dc.campaign_key                   as campaign_key,
  dd.date_key                       as date_key,
  ol.quantity                       as quantity,
  ol.discount                       as discount,
  ol.price_after_discount           as price_after_discount
from ol
join o on o.order_id = ol.order_id
left join {{ ref('eco_essentials_dim_order') }}    do on do.order_line_key = ol.order_line_id
left join {{ ref('eco_essentials_dim_product') }}  dp on dp.product_id     = ol.product_id
left join {{ ref('eco_essentials_dim_campaign') }} dc on dc.campaign_id    = ol.campaign_id
-- pick ONE of these two lines:
left join {{ ref('eco_essentials_dim_user') }}     du on du.user_key = o.customer_id
-- left join {{ ref('eco_essentials_dim_user') }}  du on try_to_number(du.customer_id) = o.customer_id
left join {{ ref('eco_essentials_dim_date') }}     dd on dd.date_key = cast(o.order_timestamp as date)
