--  Create dim_email
 
{{ config(
    materialized = 'table',
    schema = 'ecoessentials'
    )
}}
 
SELECT
emailid as email_key,
emailid as email_id,
emailname as email_name,
sendtimestamp as send_timestamp
FROM {{ source('marketing_cloud', 'ECOESSENTIALS_SALES') }}