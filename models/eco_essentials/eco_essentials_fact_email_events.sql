{{ config(materialized='table', schema='ecoessentials', alias='fact_email_events') }}

SELECT
  e.email_key,
  ev.event_key,
  d.date_key,
  u.user_key,
  c.campaign_key,
  UPPER(s.eventtype) AS event_type,
  CASE WHEN UPPER(s.eventtype) = 'SENT'  THEN 1 ELSE 0 END AS sent,
  CASE WHEN UPPER(s.eventtype) = 'OPEN'  THEN 1 ELSE 0 END AS open,
  CASE WHEN UPPER(s.eventtype) = 'CLICK' THEN 1 ELSE 0 END AS click
FROM {{ source('marketing_cloud','ECOESSENTIALS_SALES') }} s
LEFT JOIN {{ ref('eco_essentials_dim_email') }}    e  ON e.email_id      = s.emailid
LEFT JOIN {{ ref('eco_essentials_dim_event') }}    ev ON ev.event_id     = s.emaileventid
LEFT JOIN {{ ref('eco_essentials_dim_user') }}     u  ON u.email_address = s.subscriberemail
LEFT JOIN {{ ref('eco_essentials_dim_campaign') }} c  ON c.campaign_id   = s.campaignid
LEFT JOIN {{ ref('eco_essentials_dim_date') }}     d  ON d.date_key      = CAST(s.eventtimestamp AS DATE)
