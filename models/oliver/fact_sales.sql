{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
  cdim.cust_key        AS CUST_KEY,        
  ddim.date_key        AS DATE_KEY,       
  sdim.store_key       AS STORE_KEY,       
  pdim.prod_key        AS PRODUCT_KEY,     
  edim.emp_key         AS EMPLOYEE_KEY,    

  ol.QUANTITY          AS QUANTITY,
  ol.UNIT_PRICE        AS UNIT_PRICE,
  (ol.QUANTITY * ol.UNIT_PRICE) AS DOLLARS_SOLD

FROM {{ source('oliver_landing','orderline') }} ol
INNER JOIN {{ source('oliver_landing','orders') }} o
        ON o.ORDER_ID      = ol.ORDER_ID
INNER JOIN {{ ref('oliver_dim_customer') }}  cdim
        ON cdim.customerID = o.CUSTOMER_ID
INNER JOIN {{ ref('oliver_dim_employee') }}  edim
        ON edim.employeeID = o.EMPLOYEE_ID
INNER JOIN {{ ref('oliver_dim_store') }}     sdim
        ON sdim.store_ID    = o.STORE_ID
INNER JOIN {{ ref('oliver_dim_product') }}   pdim
        ON pdim.productID  = ol.PRODUCT_ID
INNER JOIN {{ ref('oliver_dim_date') }}      ddim
        ON ddim.date_key   = CAST(o.ORDER_DATE AS DATE)
