{{ config(materialized = 'incremental',
    unique_key = 'track_id',
    incremental_strategy = 'merge'
) }}

with tracks as (
    select
        t.id as track_id,
        t.name as track_name,
        t.album_id,
        t.duration_ms,
        t.explicit,
        t.popularity as track_popularity,
        t.extraction_datetime,
        t.source,
        t.timezone,
        t.artists_ids_array
    from {{ ref('staging_tracks') }} t
),

albums as (
    select
        album_id,
        album_name,
        album_release_date,
        album_total_tracks,
        album_popularity
    from {{ ref('dim_albums') }}
),

track_artist as (
    select
        track_id,
        unnest(artists_ids_array) as artist_id
    from tracks
),

artists as (
    select
        artist_id,
        artist_name
    from {{ ref('dim_artists') }}
)

select
    t.track_id,
    t.track_name,
    t.album_id,
    a.album_name,
    a.album_release_date,
    a.album_total_tracks,
    a.album_popularity,
    ta.artist_id,
    ar.artist_name,
    t.duration_ms,
    t.explicit,
    t.track_popularity,
    t.extraction_datetime,
    t.source,
    t.timezone
from tracks t
left join albums a on t.album_id = a.album_id
left join track_artist ta on t.track_id = ta.track_id
left join artists ar on ta.artist_id = ar.artist_id