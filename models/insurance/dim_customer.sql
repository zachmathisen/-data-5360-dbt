{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
)}}

with db_source as (
    select
        customerid,
        firstname,
        lastname,
        dob,
        address,
        city,
        state,
        zipcode
    from {{ source('insurance_landing', 'customers') }}
)

, cs_interactions_source as (
    select distinct
        customer_first_name,
        customer_last_name,
        customer_email
    from {{ ref('stg_customer_service_interactions') }}
)

, final as (
    select
        db.customerid,
        coalesce(db.firstname, cs.customer_first_name) as firstname,
        coalesce(db.lastname, cs.customer_last_name) as lastname,
        db.dob,
        db.address,
        db.city,
        db.state,
        db.zipcode,
        cs.customer_email
    from db_source db
    full join cs_interactions_source cs
        on db.firstname = cs.customer_first_name
        and db.lastname = cs.customer_last_name
)

select
    {{ dbt_utils.generate_surrogate_key(['firstname', 'lastname']) }} as customer_key,
    *
from final