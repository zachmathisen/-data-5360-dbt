{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
  STORE_ID   AS store_key,
  STORE_ID   AS store_ID, 
  STORE_NAME AS store_name,
  STREET     AS street,
  CITY       AS city,
  STATE      AS state

FROM {{ source('oliver_landing', 'store') }}
