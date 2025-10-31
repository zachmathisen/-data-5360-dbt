--Create dim campaign
{{ config(
    materialized = 'table',
    schema = 'ecoessentials'
    )
}}
 
select
campaign_id as campaign_key,
campaign_id as campaign_id,
campaign_name as campaign_name,
campaign_discount as campaign_discount
FROM {{ source('online_purchases', 'promotional_campaign') }}