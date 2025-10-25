{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

WITH ec AS (
  SELECT
    employee_id,
    try_parse_json(certification_json) AS cert
  FROM {{ source('oliver_landing','employee_certifications') }}
)

SELECT
  -- Keys (mirroring the style of your example)
  edim.employeeID                                           AS EMPLOYEE_KEY,
  ddim.date_key                                             AS DATE_KEY,

  -- Attributes / measures
  ec.cert:certification_name::string                        AS CERTIFICATION_NAME,
  ec.cert:certification_cost::number(10,2)                  AS CERTIFICATION_COST

FROM ec
INNER JOIN {{ ref('oliver_dim_employee') }} edim
        ON edim.employeeID = ec.employee_id
INNER JOIN {{ ref('oliver_dim_date') }} ddim
        ON ddim.date_key = TRY_TO_DATE(ec.cert:certification_awarded_date::string)
