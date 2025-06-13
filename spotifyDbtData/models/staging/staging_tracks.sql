{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id'
) }}

with source as (
    select * from {{ source('spotifyData', 'tracks') }}
),

cleaned as (
    select
        id,
        name,
        string_to_array(artist_names, ', ') as artists_array,
        string_to_array(artist_ids, ', ') as artists_ids_array,
        album_name,
        album_id,
        duration_ms::int,
        explicit::boolean,
        popularity::int,
        extraction_datetime,
        {{ split_timestamp('extraction_datetime', 'extract') }},
        source,
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
