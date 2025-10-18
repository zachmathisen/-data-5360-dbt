{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
  EMPLOYEE_ID  AS emp_key,       
  EMPLOYEE_ID  AS employeeID,    
  LAST_NAME    AS LastName,
  FIRST_NAME   AS FirstName,
  EMAIL        AS email,
  PHONE_NUMBER AS phone_number,
  POSITION     AS position,
  HIRE_DATE    AS hire_date

FROM {{ source('oliver_landing', 'employee') }}