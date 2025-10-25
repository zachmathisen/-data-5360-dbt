{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

with src as (
  select
    employee_id,
    try_parse_json(certification_json) as cert
  from {{ source('oliver_landing', 'employee_certifications') }}
),

final as (
  select
    employee_id,
    cert:certification_name::string        as certification_name,
    cert:certification_cost::number(10,2)  as certification_cost,
    try_to_date(cert:certification_awarded_date::string) as certification_awarded_date
  from src
)

select * from final
