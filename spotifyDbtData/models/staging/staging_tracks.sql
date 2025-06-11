{{ config(
    materialized = "table"
) }}


with source as (
    select * from {{ source('spotifyData', 'tracks') }}
),
cleaned as (
    select
        id,
        name,
        album,
        string_to_array(artist, ', ') as artists_array,
        duration_ms::int,
        explicit ::boolean,
        popularity::int,
        {{ split_timestamp('extraction_datetime', 'extraction_datetime') }},
        source,
        timezone
    from source
)
select * from cleaned