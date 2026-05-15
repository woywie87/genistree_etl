{{ config(materialized='table') }}

with osm_pins as (
    select
        cast('osm' as string) as pin_source,
        cast(osm_id as string) as source_record_id,
        lat,
        lon,
        historic_type,
        cast(start_date as string) as start_date,
        description,
        concat(cast(lat as string), ', ', cast(lon as string)) as lat_lon,
        concat(
            historic_type,
            ' | ',
            coalesce(description, 'brak opisu'),
            ' | ',
            coalesce(cast(start_date as string), '')
        ) as tooltip_text
    from {{ ref('stg_osm_objects') }}
    where voivodeship = 'malopolskie'
      and lat is not null
      and lon is not null
),

genistree_base as (
    select
        *,
        array_to_string(
            array(
                select trim(cast(x as string))
                from unnest([place, address, additional_info]) as x
                where x is not null and trim(cast(x as string)) != ''
            ),
            ' | '
        ) as description_raw
    from {{ ref('stg_genistree_shrines_crosses') }}
    where lat is not null
      and lon is not null
),

genistree_pins as (
    select
        cast('genistree' as string) as pin_source,
        cast(genistree_uid as string) as source_record_id,
        lat,
        lon,
        concat('typ_', cast(custom_document_type_id as string)) as historic_type,
        cast(year_raw as string) as start_date,
        nullif(description_raw, '') as description,
        concat(cast(lat as string), ', ', cast(lon as string)) as lat_lon,
        concat(
            'typ_', cast(custom_document_type_id as string),
            ' | ',
            coalesce(nullif(description_raw, ''), 'brak opisu'),
            ' | ',
            coalesce(cast(year_raw as string), '')
        ) as tooltip_text
    from genistree_base
)

select * from osm_pins

union all

select * from genistree_pins
