1. Created all nesessary connections and destinations and loaded into snowflake database "group9project".
2. The only refection from project deliverable #1 is to include a feild fro event type on the "fact_email_events" table. This will help add more dynamic capilities to the database.
3. Proccess for creating and populating your dimensional model.
3.1 creating _src_ecoessentials.yml as a recorse to grab data from tables pulled into snowflake.
    version: 2
    
    sources:
      - name: online_purchases
        database: group9project
        schema: POSTGRES_RDS_TRANSACTIONAL_DB
        quoting:
          identifier: true  
        tables:
          - name: order
            identifier: ORDER              # physical table
          - name: order_line
            identifier: ORDER_LINE         # physical table
          - name: product
            identifier: PRODUCT
          - name: customer
            identifier: CUSTOMER
          - name: promotional_campaign
            identifier: PROMOTIONAL_CAMPAIGN
    
    
      - name: marketing_cloud
        database: group9project
        schema: s3
        tables:
          - name: ECOESSENTIALS_SALES

3.1 Create the dimential tables of dim_product, dim_order, dim_date, dim_user, dim_campaign, dim_email, dim_event, fact_sales, and fact_email_events. Using this file nameing convention: eco_essentials_dim_XXXXX

3.1.1  eco_essentials_dim_product
    {{ config(
        materialized = 'table',
        schema = 'ecoessentials'
    ) }}
    SELECT
      product_id        AS prod_key,
      product_id        AS product_id,
      product_type      AS product_type,
      product_name      AS product_name,
      price             AS price

FROM {{ source('online_purchases', 'product') }}

3.1.2  eco_essentials_dim_date
    {{ config(
        materialized = 'table',
        schema = 'ecoessentials'
        )
    }}
     
    with cte_date as (
    {{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
    )
    SELECT
    date_day as date_key,
    date_day,
    day_of_week,
    month_of_year,
    quarter_of_year,
    year_number
    from cte_date
3.1.3  eco_essentials_dim_date
    {{ config(
        materialized = 'table',
        schema = 'ecoessentials'
        )
    }} 
    select
    campaign_id as campaign_key,
    campaign_id as campaign_id,
    campaign_name as campaign_name,
    campaign_discount as campaign_discount
    FROM {{ source('online_purchases', 'promotional_campaign') }}

3.1.4  eco_essentials_dim_order
    {{ config(materialized='table', schema='ecoessentials', alias='dim_order') }}
    
    with order_lines as (
      select
        order_line_id,
        order_id,
        quantity,
        discount,
        price_after_discount
      from {{ source('online_purchases','order_line') }}
    ),
    orders as (
      select
        order_id,
        -- adjust to your actual column name; alias to a consistent name
        order_timestamp as order_timestamp
      from {{ source('online_purchases','order') }}
    )
    select
      ol.order_line_id        as order_line_key,
      ol.order_id             as order_id,
      ol.quantity             as quantity,
      ol.discount             as discount,
      ol.price_after_discount as price_after_discount,
      o.order_timestamp       as order_timestamp
    from order_lines ol
    left join orders o
      on o.order_id = ol.order_id
3.1.4  eco_essentials_dim_email
    {{ config(
        materialized = 'table',
        schema = 'ecoessentials'
        )
    }}
    SELECT
    emailid as email_key,
    emailid as email_id,
    emailname as email_name,
    sendtimestamp as send_timestamp
    FROM {{ source('marketing_cloud', 'ECOESSENTIALS_SALES') }}

3.1.5 eco_essentials_dim_event
    {{ config(
        materialized = 'table',
        schema = 'ecoessentials'
        )
    }}
    select
    emaileventid as event_key,
    emaileventid as event_id,
    eventtype as event_type,
    eventtimestamp as event_timestamp
    FROM {{ source('marketing_cloud', 'ECOESSENTIALS_SALES') }}

3.1.6 eco_essentials_dim_user
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

Creating fact tables email_events and sales
3.2.1 eco_essentials_fact_email_events
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

3.2.2 eco_essentials_fact_sales
    {{ config(materialized='table', schema='ecoessentials', alias='fact_sales') }}
    with ol as (
      select order_line_id, order_id, product_id, campaign_id, quantity, discount, price_after_discount
      from {{ source('online_purchases','order_line') }}
    ),
    o as (
      select order_id, customer_id, order_timestamp
      from {{ source('online_purchases','order') }}
    )
    select
      do.order_line_key                 as order_line_key,
      dp.prod_key                    as prod_key,
      du.user_key                       as user_key,          -- may be NULL for marketing-only users
      dc.campaign_key                   as campaign_key,
      dd.date_key                       as date_key,
      ol.quantity                       as quantity,
      ol.discount                       as discount,
      ol.price_after_discount           as price_after_discount
    from ol
    join o on o.order_id = ol.order_id
    left join {{ ref('eco_essentials_dim_order') }}    do on do.order_line_key = ol.order_line_id
    left join {{ ref('eco_essentials_dim_product') }}  dp on dp.product_id     = ol.product_id
    left join {{ ref('eco_essentials_dim_campaign') }} dc on dc.campaign_id    = ol.campaign_id
    -- pick ONE of these two lines:
    left join {{ ref('eco_essentials_dim_user') }}     du on du.user_key = o.customer_id
    -- left join {{ ref('eco_essentials_dim_user') }}  du on try_to_number(du.customer_id) = o.customer_id
    left join {{ ref('eco_essentials_dim_date') }}     dd on dd.date_key = cast(o.order_timestamp as date)

3.3.1 Creating the Ecoessentials database schema: 
    version: 2
    
    models:
      # -------------------- Dimensions --------------------
      - name: eco_essentials_dim_product
        description: "Product dimension for EcoEssentials, containing product details."
    
      - name: eco_essentials_dim_order
        description: "Order-line dimension with individual line item details per order."
    
      - name: eco_essentials_dim_user
        description: "User dimension that merges online purchase customers with marketing subscribers."
    
      - name: eco_essentials_dim_campaign
        description: "Campaign dimension capturing promotional campaign details."
    
      - name: eco_essentials_dim_email
        description: "Email dimension storing information about email sends."
    
      - name: eco_essentials_dim_event
        description: "Event dimension storing metadata about email events."
    
      - name: eco_essentials_dim_date
        description: "Date dimension providing time intelligence attributes."
    
      # ---------------------- Facts -----------------------
      - name: eco_essentials_fact_sales
        description: "Sales fact table at the order_line grain, connecting products, users, and campaigns."
    
      - name: eco_essentials_fact_email_events
        description: "Email engagement fact table at the email event grain, connecting users, campaigns, and dates."


4. Submit Screeshot of Dimensional Model
<img width="1500" height="850" alt="image" src="https://github.com/user-attachments/assets/4687fbf5-8cb7-4b54-ad29-cdb0b24879b8" />

<img width="1502" height="850" alt="image" src="https://github.com/user-attachments/assets/f1e9419f-c0dc-4547-9c9f-db9d0e376ffc" />


5. Three Business Questions
  5.1. What is the top 5 preforming email by clicks we had? 
        SELECT TOP 5 dem.email_name, SUM(fee.click) AS clicks
        FROM ecoessentials.fact_email_events AS fee
        JOIN ecoessentials.eco_essentials_dim_email AS dem
          ON fee.email_key = dem.email_key
        GROUP BY dem.email_name
        ORDER BY clicks DESC;
<img width="2064" height="1556" alt="image" src="https://github.com/user-attachments/assets/41c474b8-7879-423d-94bb-504bbd9f6c42" />

  5.2. What Campaign resulted in the most sales? 

          USE DATABASE GROUP9PROJECT ;
        USE SCHEMA GROUP9PROJECT.ECOESSENTIALS;
         
        SELECT c.campaign_name , SUM(s.PRICE_AFTER_DISCOUNT)
        FROM ECO_ESSENTIALS_DIM_CAMPAIGN c 
            JOIN FACT_SALES s 
            ON c.campaign_key = s.campaign_key
        GROUP BY c.campaign_name
        ORDER BY SUM(s.PRICE_AFTER_DISCOUNT) DESC
        LIMIT 1    
<img width="2300" height="1046" alt="image" src="https://github.com/user-attachments/assets/07810d20-4dcd-418b-8889-e43f9db9b1d8" />
     
  5.3. What was the total discount that we gave to customers?
        SELECT SUM(s.discount)  AS total_discount
        FROM FACT_SALES s
            JOIN ECO_ESSENTIALS_DIM_DATE d
            ON s.date_key = d.date_key
<img width="2308" height="842" alt="image" src="https://github.com/user-attachments/assets/e8e24c1a-3c38-4ade-93cc-8f7a9ebd9e4e" />

    
