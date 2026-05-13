{{
    config(
        materialized='view'
    )
}}

/*
    Staging: campaigns
    - Pass-through with basic cleaning
*/

select
    campaign_id,
    campaign_name,
    advertiser_id,
    advertiser_name,
    campaign_start_date,
    campaign_end_date,
    campaign_budget_usd,
    campaign_status,
    targeting_device_types,
    targeting_countries,
    created_at,
    _loaded_at
from {{ source('raw', 'campaigns') }}
