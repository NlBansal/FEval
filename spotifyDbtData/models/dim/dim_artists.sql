{{ config(
        materialized = 'incremental',
        unique_key = 'artist_id',
        incremental_strategy = 'merge'
) }}

select distinct
    artist_id,
    name as artist_name
from {{ ref('all_artists_data') }}
where artist_id is not null