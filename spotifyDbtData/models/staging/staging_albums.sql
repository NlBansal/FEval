{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id'
) }}

with source as (
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
),

filtered as (
    select *
    from cleaned
    {% if is_incremental() %}
        where extraction_datetime > (
            select coalesce(max(extraction_datetime), '1900-01-01')
            from {{ this }}
        )
    {% endif %}
)

select * from filtered
