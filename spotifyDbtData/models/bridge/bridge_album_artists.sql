{{ config(materialized = 'table') }}

select
    id as album_id,
    unnest(artists_array) as artist_name
from {{ ref('staging_albums') }}
where artists_array is not null