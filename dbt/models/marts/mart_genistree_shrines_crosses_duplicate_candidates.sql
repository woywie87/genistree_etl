{{ config(materialized='table') }}

with genistree_points as (
    select
        genistree_uid,
        custom_document_type_id,
        concat('typ_', cast(custom_document_type_id as string)) as object_type,
        place,
        address,
        additional_info,
        year_raw,
        lat,
        lon,
        st_geogpoint(lon, lat) as geom
    from {{ ref('stg_genistree_shrines_crosses') }}
    where lat is not null
      and lon is not null
      and lat between -90 and 90
      and lon between -180 and 180
),

candidate_pairs as (
    select
        a.genistree_uid as genistree_uid_a,
        b.genistree_uid as genistree_uid_b,
        a.custom_document_type_id as custom_document_type_id_a,
        b.custom_document_type_id as custom_document_type_id_b,
        a.object_type as object_type_a,
        b.object_type as object_type_b,
        a.place as place_a,
        b.place as place_b,
        a.address as address_a,
        b.address as address_b,
        a.additional_info as additional_info_a,
        b.additional_info as additional_info_b,
        cast(a.year_raw as string) as year_a,
        cast(b.year_raw as string) as year_b,
        a.lat as lat_a,
        a.lon as lon_a,
        b.lat as lat_b,
        b.lon as lon_b,
        round(st_distance(a.geom, b.geom), 2) as distance_m
    from genistree_points as a
    inner join genistree_points as b
        on a.genistree_uid < b.genistree_uid
       and a.custom_document_type_id = b.custom_document_type_id
       and st_dwithin(a.geom, b.geom, 50)
)

select
    *,
    case
        when distance_m <= 5 then '0-5m'
        when distance_m <= 10 then '5-10m'
        when distance_m <= 25 then '10-25m'
        else '25-50m'
    end as distance_bucket
from candidate_pairs
order by distance_m, genistree_uid_a, genistree_uid_b
