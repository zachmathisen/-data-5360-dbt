{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
    fs.CUST_KEY,
    c.first_name,
    c.last_name,
    c.state AS customer_state,

    fs.STORE_KEY,
    s.store_name,
    s.city AS store_city,
    s.state AS store_state,

    fs.PROD_KEY,
    p.product_name,
    p.unit_price,

    fs.EMPLOYEE_KEY,
    e.first_name AS employee_first_name,
    e.last_name  AS employee_last_name,
    e.position,

    fs.DATE_KEY,
    d.year_number,
    d.month_name,
    d.quarter_of_year,

    fs.QUANTITY,
    fs.UNIT_PRICE,
    fs.DOLLARS_SOLD

FROM {{ ref('fact_sales') }} fs
INNER JOIN {{ ref('oliver_dim_customer') }} c ON fs.CUST_KEY = c.cust_key
INNER JOIN {{ ref('oliver_dim_store') }}    s ON fs.STORE_KEY = s.store_key
INNER JOIN {{ ref('oliver_dim_product') }}  p ON fs.PROD_KEY  = p.prod_key
INNER JOIN {{ ref('oliver_dim_employee') }} e ON fs.EMPLOYEE_KEY = e.emp_key
INNER JOIN {{ ref('oliver_dim_date') }}     d ON fs.DATE_KEY = d.date_key
