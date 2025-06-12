{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'artist_sk',
    indexes = [
        {"columns": ["artist_sk"]}
    ],
    pre_hook = [
        "CREATE SEQUENCE IF NOT EXISTS artist_seq START WITH 1 INCREMENT BY 1;",
        "{% if is_incremental() %}ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_artist_sk;{% endif %}"
    ],
    post_hook = [
        "{% if is_incremental() %}ALTER TABLE {{ this }} ADD CONSTRAINT pk_artist_sk PRIMARY KEY (artist_sk);{% endif %}"
    ]
) }}

with album_artists as (
    select unnest(artists_array) as name
    from {{ ref('staging_albums') }}
),

track_artists as (
    select unnest(artists_array) as name
    from {{ ref('staging_tracks') }}
),

raw_artist_names as (
    select trim(name) as name from album_artists
    union
    select trim(name) as name from track_artists
),

staging_artist_data as (
    select
        id as artist_id,
        trim(name) as name,
        popularity
    from {{ ref('staging_artists') }}
),

final_artists as (
    select artist_id, name, popularity from staging_artist_data
    union
    select null as artist_id, name, null as popularity
    from raw_artist_names
    where name not in (select name from staging_artist_data)
)

select
    nextval('artist_seq') as artist_sk,
    artist_id,
    name,
    popularity
from final_artists
