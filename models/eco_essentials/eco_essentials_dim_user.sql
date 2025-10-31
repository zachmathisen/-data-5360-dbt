{{ config(
    materialized = 'table',
    schema = 'ecoessentials'
) }}

SELECT
  c.customer_id          AS user_key,
  s.customerid           AS customer_id,
  s.subscriberemail      AS email_address,
  s.subscriberfirstname  AS first_name,
  s.subscriberlastname   AS last_name,
  c.customer_phone       AS user_phone,
  c.customer_address     AS user_address,
  c.customer_city        AS user_city,
  c.customer_state       AS user_state,
  c.customer_zip         AS user_zip,
  c.customer_country     AS user_country
FROM {{ source('marketing_cloud', 'ECOESSENTIALS_SALES') }} s
LEFT JOIN {{ source('online_purchases', 'customer') }} c
  ON TO_VARCHAR(c.customer_id) = s.customerid
