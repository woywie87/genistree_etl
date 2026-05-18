{{ config(materialized='table') }}

{# Agregacja timeline: dekada smierci x przedzial wieku (age_effective). Bez wierszy bez sensownego wieku. #}
with base as (
    select
        death_decade_year,
        age_effective
    from {{ ref('int_genistree_person_death_decade_century') }}
    where age_effective is not null
      and age_effective between 0 and 130
),

labeled as (
    select
        death_decade_year,
        case
            when age_effective <= 17 then '0-17'
            when age_effective <= 44 then '18-44'
            when age_effective <= 64 then '45-64'
            else '65_plus'
        end as age_bucket_code,
        case
            when age_effective <= 17 then 1
            when age_effective <= 44 then 2
            when age_effective <= 64 then 3
            else 4
        end as age_bucket_sort
    from base
)

select
    death_decade_year as decade_year,
    age_bucket_code,
    case age_bucket_code
        when '0-17' then 'Dzieci i młodzież (0–17 lat)'
        when '18-44' then 'Dorośli (18–44 lata)'
        when '45-64' then 'Dorośli (45–64 lata)'
        else 'Seniorzy (65+ lat)'
    end as age_bucket_label,
    min(age_bucket_sort) as age_bucket_sort,
    count(*) as persons_count
from labeled
group by 1, 2
order by decade_year, age_bucket_sort
