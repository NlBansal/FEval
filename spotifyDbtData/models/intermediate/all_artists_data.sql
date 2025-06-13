{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = 'artist_sk',
    indexes = [
      {"columns": ["name"]}
    ],
    pre_hook = [
      "CREATE SEQUENCE IF NOT EXISTS intermediate.artist_seq START WITH 1 INCREMENT BY 1;",
      "{% if not is_incremental() %}ALTER SEQUENCE IF EXISTS intermediate.artist_seq RESTART WITH 1;{% endif %}",
      "{% if is_incremental() %}ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_artist_sk;{% endif %}"
    ],
    post_hook = [
      "{% if is_incremental() %}ALTER TABLE {{ this }} ADD CONSTRAINT pk_artist_sk PRIMARY KEY (artist_sk);{% endif %}"
    ]
  ) 
}}
with album_artists as (
  select
    t.name,
    t.artist_id
  from {{ ref('staging_albums') }} a,
  lateral unnest(
    ARRAY(
      select row(trim(names.name), trim(ids.id))
      from unnest(coalesce(a.artists_array, ARRAY[]::text[])) with ordinality as names(name, ord)
      left join unnest(coalesce(a.artists_ids_array, ARRAY[]::text[])) with ordinality as ids(id, ord)
        on names.ord = ids.ord
    )
  ) as t(name text, artist_id text)
),

track_artists as (
  select
    t.name,
    t.artist_id
  from {{ ref('staging_tracks') }} a,
  lateral unnest(
    ARRAY(
      select row(trim(names.name), trim(ids.id))
      from unnest(coalesce(a.artists_array, ARRAY[]::text[])) with ordinality as names(name, ord)
      left join unnest(coalesce(a.artists_ids_array, ARRAY[]::text[])) with ordinality as ids(id, ord)
        on names.ord = ids.ord
    )
  ) as t(name text, artist_id text)
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
  select
    artist_id,
    r.name,
    null as popularity
  from raw_artist_names r
  left join staging_artist_data s on r.name = s.name
  where s.name is null
)

select
  nextval('intermediate.artist_seq') as artist_sk,
  artist_id,
  name,
  popularity
from final_artists
