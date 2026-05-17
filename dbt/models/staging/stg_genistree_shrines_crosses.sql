{{ config(materialized='view') }}

select
    _uid as genistree_uid,
    safe_cast(CustomDocumentTypeID as int64) as custom_document_type_id,
    nullif(trim(cast(Place as string)), '') as place,
    nullif(trim(cast(Address as string)), '') as address,
    nullif(trim(cast(AdditionalInfo as string)), '') as additional_info,
    `Year` as year_raw,
    safe_cast(GeoLat as float64) as lat,
    safe_cast(GeoLon as float64) as lon
from {{ source('RAW', 'GENISTREE_OBJECTS') }}
where safe_cast(CustomDocumentTypeID as int64) between 1 and 5
