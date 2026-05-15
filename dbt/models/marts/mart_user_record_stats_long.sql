{{ config(materialized='table') }}

with genistree_by_type as (
    select
        cast(CreateUserID as string) as create_user_id,
        concat('typ_', coalesce(cast(CustomDocumentTypeID as string), 'unknown')) as record_type,
        count(*) as records_count
    from {{ source('RAW', 'GENISTREE_OBJECTS') }}
    where CreateUserID is not null
    group by 1, 2
),

census_revision_books as (
    select
        cast(CreateUserID as string) as create_user_id,
        'spisy_rewizyjne' as record_type,
        count(*) as records_count
    from {{ source('RAW', 'GENISTREE_CENSUS') }}
    where CreateUserID is not null
    group by 1
)

select
    create_user_id,
    record_type,
    records_count
from genistree_by_type

union all

select
    create_user_id,
    record_type,
    records_count
from census_revision_books
