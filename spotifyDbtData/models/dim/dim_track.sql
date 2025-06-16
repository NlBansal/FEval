{{ config(
    materialized = 'incremental',
    unique_key = 'track_id',
    incremental_strategy = 'merge'
) }}

select distinct
    id as track_id,
    name as track_name
from {{ ref('staging_tracks') }}