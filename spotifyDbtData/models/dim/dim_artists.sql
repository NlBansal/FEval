{{ config(materialized = 'table') }}

select distinct
    artist_id,
    name as artist_name
from {{ ref('all_artists_data') }}
where artist_id is not null