{{ config(materialized='view') }}

with ranked as (

    select

        event_id,
        event_type,
        event_timestamp,
        toDate(event_timestamp) as event_date,
        publisher_id,
        ad_unit_id,
        campaign_id,
        advertiser_id,
        device_type,
        browser,
        upper(country_code) as country_code,
        region,
        placement_quality_score,
        revenue_usd,
        bid_floor_usd,
        is_filled,
        site_domain,
        _source_file,
        _loaded_at,
        row_number() over (
            partition by event_id
            order by _loaded_at desc
        ) as rn

    from {{ source('raw', 'ad_events') }}

    where event_id != ''
      and event_type != ''

)

select
    event_id,
    event_type,
    event_timestamp,
    event_date,
    publisher_id,
    ad_unit_id,
    campaign_id,
    advertiser_id,
    device_type,
    browser,
    country_code,
    region,
    placement_quality_score,
    revenue_usd,
    bid_floor_usd,
    is_filled,
    site_domain,
    _source_file,
    _loaded_at
from ranked
where rn = 1