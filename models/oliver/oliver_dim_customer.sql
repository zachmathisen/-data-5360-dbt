{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
  CUSTOMER_ID  AS cust_key,      
  CUSTOMER_ID  AS customerID,  
  LAST_NAME    AS last_name,
  EMAIL        AS email,
  PHONE_NUMBER AS phone_number,
  STATE        AS state

FROM {{ source('oliver_landing', 'customer') }}