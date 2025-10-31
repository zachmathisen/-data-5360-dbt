-- Create dim event table
 
{{ config(
    materialized = 'table',
    schema = 'ecoessentials'
    )
}}
 
select
emaileventid as event_key,
emaileventid as event_id,
eventtype as event_type,
eventtimestamp as event_timestamp
FROM {{ source('marketing_cloud', 'ECOESSENTIALS_SALES') }}