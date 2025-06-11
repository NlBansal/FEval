{{ config(
    materialized = "table",
) }}
with source as (
    select * from {{ source('spotifyData', 'artists') }}
),
cleaned as (
    select
        id,
        name,
        popularity::int,
        genres,
        {{ split_timestamp('extraction_datetime', 'extraction_datetime') }},
        source,
        data_version,
        timezone
    from source
)
select * from cleaned