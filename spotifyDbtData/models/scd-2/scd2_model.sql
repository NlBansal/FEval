{{ config(
    materialized = 'incremental',
    unique_key = 'scd_id',
    incremental_strategy = 'merge'
) }}

with source as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'track_id',
            'artist_id',
            'album_id',
            'duration_ms',
            'explicit',
            'track_popularity'
        ]) }} as scd_id
    from {{ ref('track_fact') }}
),

final as (
    select
        scd_id,
        track_id,
        track_name,
        album_id,
        album_name,
        album_release_date,
        album_total_tracks,
        album_popularity,
        artist_id,
        artist_name,
        duration_ms,
        explicit,
        track_popularity,
        extraction_datetime as valid_from,
        lead(extraction_datetime) over (
            partition by track_id, artist_id
            order by extraction_datetime
        ) as valid_to,
        source,
        timezone
    from source
)

select
    *,
    case when valid_to is null then true else false end as is_current
from final