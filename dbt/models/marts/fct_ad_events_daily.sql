{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='event_date'
    )
}}

with daily as (

    select
        event_date,
        publisher_id,
        campaign_id,
        device_type,
        country_code,

        countIf(event_type = 'impression') as impressions,

        countIf(event_type = 'click') as clicks,

        countIf(event_type = 'viewable_impression') as viewable_impressions,

        sumIf(revenue_usd, revenue_usd is not null) as revenue_usd,

        round(
            countIf(is_filled = 1) / nullif(count(), 0),
            4
        ) as fill_rate

    from {{ ref('stg_ad_events') }}

    group by
        event_date,
        publisher_id,
        campaign_id,
        device_type,
        country_code

)

select *
from daily

{% if is_incremental() %}

where event_date >= today() - 7

{% endif %}