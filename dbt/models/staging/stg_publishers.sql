{{
    config(
        materialized='view'
    )
}}

/*
    Staging: publishers
    - Pass-through with basic cleaning
*/

select
    publisher_id,
    publisher_name,
    publisher_category,
    primary_domain,
    account_manager,
    country,
    timezone,
    created_at,
    updated_at,
    _loaded_at
from {{ source('raw', 'publishers') }}
