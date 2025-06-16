{{ config(  materialized = 'incremental',
            unique_key = 'date_id', 
            incremental_strategy = 'merge',
            indexes = [
                {"columns": ["date_id"], "name": "idx_date_actual"}
            ]
) }}

with normalized_dates as (
    select
        case
            when length(release_date) = 4 then release_date || '-01-01'
            when length(release_date) = 7 then release_date || '-01'
            else release_date
        end as normalized_release_date
    from {{ ref('staging_albums') }}
    where release_date is not null
),

date_range as (
    select
        min(cast(normalized_release_date as date)) as min_date,
        max(cast(normalized_release_date as date)) as max_date
    from normalized_dates
),

calendar as (
    select
        generate_series(min_date, max_date, interval '1 day') as date_actual
    from date_range
)

select
    to_char(date_actual, 'YYYYMMDD')::int as date_id,
    date_actual as date,
    extract(year from date_actual) as year,
    extract(month from date_actual) as month,
    extract(day from date_actual) as day,
    extract(dow from date_actual) as day_of_week,
    extract(doy from date_actual) as day_of_year,
    extract(week from date_actual) as week_of_year,
    extract(quarter from date_actual) as quarter
from calendar
order by date_actual