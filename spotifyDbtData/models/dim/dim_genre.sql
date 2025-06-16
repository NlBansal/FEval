{{ config(
    materialized = 'incremental',
    unique_key = 'genre_id',
    incremental_strategy = 'merge',
    indexes = [
        {"columns": ["genre_id"]}
    ]
) }}

with source as (
    select
        id as artist_id,
        unnest(string_to_array(genres, ',')) as genre,
        extraction_datetime
    from {{ ref('staging_artists') }}
),

distinct_genres as (
    select distinct
        trim(genre) as genre
    from source
    where genre is not null and trim(genre) <> ''
)

select
    {{ dbt_utils.generate_surrogate_key(['genre']) }} as genre_id,
    genre
from distinct_genres