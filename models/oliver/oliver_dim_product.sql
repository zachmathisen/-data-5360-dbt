{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
  PRODUCT_ID  AS prod_key,      
  PRODUCT_ID  AS productID,    
  PRODUCT_NAME AS product_name,
  DESCRIPTION  AS description

FROM {{ source('oliver_landing', 'product') }}
