{{ config(materialized='view') }}

{# Rok smierci jako proxy (brak daty pochowania w Persons). Dekada = pierwszy rok (1940). #}
{# Koniec wieku gregorianskiego: np. 2000 dla lat 1901–2000. #}
select
    genistree_uid,
    address,
    geo_lat,
    geo_lon,
    first_name,
    last_name,
    maiden_name,
    birth_year,
    death_year,
    age,
    age_calculated,
    age_effective,
    add_info,
    div(death_year, 10) * 10 as death_decade_year,
    div(death_year - 1, 100) * 100 + 100 as death_century_end_year
from {{ ref('stg_genistree_persons') }}
where death_year is not null
  and death_year between 1000 and 2100
