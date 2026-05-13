{{
    config(
        materialized='view'
    )
}}

/*
    Staging: ad_events
    - Pass-through over raw.ad_events.
    - Normalizes country_code to uppercase.
    - Does NOT deduplicate. Re-delivered events appear as multiple rows
      with the same event_id and a later _loaded_at — downstream models
      are responsible for choosing the dedup semantics.
*/

select
    event_id,
    event_type,
    event_timestamp,
    publisher_id,
    site_domain,
    ad_unit_id,
    campaign_id,
    advertiser_id,
    device_type,
    upper(country_code) as country_code,
    region,
    browser,
    revenue_usd,
    bid_floor_usd,
    is_filled,
    _source_file,
    _loaded_at
from {{ source('raw', 'ad_events') }}
where event_id != ''
  and event_type != ''
