with 

source as (

    select * from {{ source('crm', 'CRM__ORDERS') }}

),

renamed as (

    select
        try_to_numeric(campaign_id) as campaign_id,
        campaign_name,
        datetime as datetime_in_timezone,
        timezone,
        CONVERT_TIMEZONE(tz.timezone_name, 'UTC', datetime) as datetime_in_UTC,
        total_amount as total_amount_in_currency,
        currency,
        round(total_amount * coalesce(cr.currency_rate, 1), 2) as total_amount_in_USD

    from source s
    left join {{ ref("seed_time_zone_codes") }} tz on tz.timezone_code = s.timezone
    left join {{ ref("seed_currency_rate") }} cr on cr.currency_code = s.currency
    where try_to_numeric(campaign_id) is not null

)

select * from renamed
