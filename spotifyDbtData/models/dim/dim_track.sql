{{ config(materialized = 'table') }}

select distinct
    id as track_id,
    name as track_name
from {{ ref('staging_tracks') }}