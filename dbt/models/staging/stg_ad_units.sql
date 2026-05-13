{{
    config(
        materialized='view'
    )
}}

/*
    Staging: ad_units
    - Pass-through with basic cleaning
*/

select
    ad_unit_id,
    publisher_id,
    ad_unit_name,
    ad_format,
    ad_size,
    placement_type,
    is_active,
    created_at,
    _loaded_at
from {{ source('raw', 'ad_units') }}
