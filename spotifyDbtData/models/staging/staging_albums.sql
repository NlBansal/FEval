{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
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
        release_date,
        string_to_array(artist_names, ', ') as artists_array,
        string_to_array(artist_ids, ', ') as artists_ids_array,
        string_to_array(track_names, ', ') as tracks_array,
        string_to_array(track_ids, ', ') as tracks_ids_array,
        extraction_datetime,
        {{ split_timestamp('extraction_datetime', 'extract') }},
        source,
        extractor,
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
