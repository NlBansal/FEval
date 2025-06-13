{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id'
) }}

with source as (
    select * from {{ source('spotifyData', 'artists') }}
),

cleaned as (
    select
        id,
        name,
        string_to_array(genres, ', ') as genres_array,
        popularity::int,
        extraction_datetime,
        {{ split_timestamp('extraction_datetime', 'extract') }},
        source,
        data_version,
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
