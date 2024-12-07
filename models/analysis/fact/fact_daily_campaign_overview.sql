
with orders as (
    select cast(datetime_in_UTC as date) as date_in_UTC
        , campaign_id
        , sum(total_amount_in_USD) as total_orders_amount_in_USD
    from {{ ref("cleansed_crm__orders")}}
    group by cast(datetime_in_UTC as date)
        , campaign_id
)
, ads_spend as (
    select cast(datetime_in_UTC as date) as date_in_UTC
        , campaign_id
        , sum(ad_spend_in_USD) as total_ad_spend_in_USD
    from {{ ref("cleansed_google__ads_spend") }}
    group by cast(datetime_in_UTC as date)
        , campaign_id
)
select o.date_in_UTC
    , o.campaign_id
    , o.total_orders_amount_in_USD
    , a.total_ad_spend_in_USD
from orders o
left join ads_spend a on a.date_in_UTC = o.date_in_UTC and a.campaign_id = o.campaign_id
order by 1, 2