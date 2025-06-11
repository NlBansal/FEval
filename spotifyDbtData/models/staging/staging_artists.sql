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
        extraction_datetime,
        {{ split_timestamp('extraction_datetime', 'extract') }},
        source,
        data_version,
        timezone
    from source
)
select * from cleaned