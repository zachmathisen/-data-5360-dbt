{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
)}}

select
    cu.customer_key,
    a.agent_key,
    d.date_key,
    c.call_duration_min,
    c.issue_type,
    c.resolution_status
from {{ ref('stg_customer_service_interactions') }} c
inner join {{ ref('dim_customer') }} cu
    on c.customer_first_name = cu.firstname
    and c.customer_last_name = cu.lastname
inner join {{ ref('dim_agent') }} a
    on c.agent_first_name = a.firstname
    and c.agent_last_name = a.lastname
inner join {{ ref('dim_date') }} d
    on d.date_key = c.interaction_date
