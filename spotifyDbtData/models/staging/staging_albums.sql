{{ config(
    materialized = "table",
) }}

with source as(
    select * from {{ source('spotifyData', 'albums') }}
),
cleaned as (
    select
        id,
        name,
        release_date,
        total_tracks::int,
        popularity::int,
        string_to_array(artists, ', ') as artists_array,
        string_to_array(tracks, ', ') as tracks_array,
        extraction_datetime,
        {{ split_timestamp('extraction_datetime', 'extract') }},
        source,
        data_version,
        timezone
    from source
)
select * from cleaned