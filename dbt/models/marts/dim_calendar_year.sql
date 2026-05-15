{{ config(materialized='table') }}

select
    y as calendar_year,
    floor(y / 10) * 10 as decade_start,
    floor(y / 100) * 100 as century_start,
    concat(
        cast(floor(y / 10) * 10 as string),
        '–',
        cast(floor(y / 10) * 10 + 9 as string)
    ) as decade_label
from unnest(generate_array(1600, 2030)) as y
