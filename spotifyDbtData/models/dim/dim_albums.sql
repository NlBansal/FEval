{{ config(materialized = 'table') }}

select distinct
    id as album_id,
    name as album_name,
    release_date as album_release_date,
    total_tracks as album_total_tracks,
    popularity as album_popularity
from {{ ref('staging_albums') }}