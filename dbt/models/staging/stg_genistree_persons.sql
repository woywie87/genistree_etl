{{ config(materialized='table') }}

with genistree_documents as (
    select
        _uid        as genistree_uid,
        Address       as address,
        GeoLat      as geo_lat,
        GeoLon      as geo_lon,
        Persons     as persons_json
    from {{ source('RAW', 'GENISTREE_OBJECTS') }}
    where Persons is not null
      and Persons != '[]'
),

unpacked as (
    select
        genistree_uid,
        address,
        geo_lat,
        geo_lon,
        JSON_EXTRACT_SCALAR(person, '$.FirstName')  as first_name,
        JSON_EXTRACT_SCALAR(person, '$.LastName')   as last_name,
        JSON_EXTRACT_SCALAR(person, '$.MaidenName') as maiden_name,
        JSON_EXTRACT_SCALAR(person, '$.BirthYear')  as birth_year_raw,
        JSON_EXTRACT_SCALAR(person, '$.DeathYear')  as death_year_raw,
        JSON_EXTRACT_SCALAR(person, '$.Age')        as age_raw,
        JSON_EXTRACT_SCALAR(person, '$.AddInfo')    as add_info
    from genistree_documents,
    UNNEST(JSON_EXTRACT_ARRAY(persons_json)) as person
),

parsed as (
    select
        genistree_uid,
        address,
        SAFE_CAST(geo_lat AS FLOAT64)                           as geo_lat,
        SAFE_CAST(geo_lon AS FLOAT64)                           as geo_lon,
        TRIM(first_name)                                        as first_name,
        TRIM(last_name)                                         as last_name,
        TRIM(maiden_name)                                       as maiden_name,
        SAFE_CAST(REGEXP_EXTRACT(birth_year_raw, r'\d{4}') AS INT64) as birth_year,
        SAFE_CAST(REGEXP_EXTRACT(death_year_raw, r'\d{4}') AS INT64) as death_year,
        SAFE_CAST(NULLIF(TRIM(age_raw), '') AS INT64)           as age,
        NULLIF(TRIM(add_info), '')                              as add_info
    from unpacked
    where last_name is not null
      and last_name != ''
),

with_calculated_age as (
    select
        *,
        case
            when birth_year is not null
                and death_year is not null
                and death_year >= birth_year
                and (death_year - birth_year) <= 130
            then death_year - birth_year
        end as age_calculated
    from parsed
)

select
    *,
    coalesce(age, age_calculated) as age_effective
from with_calculated_age
