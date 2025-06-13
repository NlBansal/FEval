{{ config(materialized = 'table') }}

select
    id as track_id,
    unnest(artists_ids_array) as artist_name
from {{ ref('staging_tracks') }}
where artists_ids_array is not null