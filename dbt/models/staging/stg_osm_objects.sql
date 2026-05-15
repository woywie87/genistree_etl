{{ config(materialized='view') }}

select
    osm_id,
    osm_type,
    historic_type,
    safe_cast(lat as float64) as lat,
    safe_cast(lon as float64) as lon,
    name,
    material,
    start_date,
    description,
    voivodeship,
    fetched_at
from {{ source('RAW', 'OSM_OBJECTS') }}
