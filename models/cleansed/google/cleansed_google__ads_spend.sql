with 

source as (

    select * from {{ source('google', 'GOOGLE__ADS_SPEND') }}

),

renamed as (

    select
        datetime as datetime_in_timezone,
        timezone,
        CONVERT_TIMEZONE(tz.timezone_name, 'UTC', datetime) as datetime_in_UTC,
        ad_spend as ad_spend_in_currency,
        currency,
        cast(round(ad_spend * coalesce(cr.currency_rate, 1), 2) as numeric(38,2)) as ad_spend_in_USD,
        try_to_numeric(substring(split_part(labels, ',', 1), 5)) as campaign_id,
        labels

    from source s
    left join {{ ref("seed_time_zone_codes") }} tz on tz.timezone_code = s.timezone
    left join {{ ref("seed_currency_rate") }} cr on cr.currency_code = s.currency

)

select * from renamed
