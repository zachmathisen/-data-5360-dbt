{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
)}}

select
    interaction_id, 
    split_part(customer_name, ' ', 1) as customer_first_name,
    split_part(customer_name, ' ', 2) as customer_last_name,
    customer_email, 
    split_part(agent_name, ' ', 1) as agent_first_name,
    split_part(agent_name, ' ', 2) as agent_last_name,
    agent_email, 
    interaction_date, 
    channel, 
    issue_type, 
    resolution_status, 
    call_duration_min
from {{ source('insurance_landing', 'customer_service_interactions')}}