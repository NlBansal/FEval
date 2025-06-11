{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'name',
    pre_hook = [
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE constraint_name = 'pk_artist_id'
                  AND table_schema = '{{ this.schema }}'
                  AND table_name = '{{ this.identifier }}'
            ) THEN
                EXECUTE format('ALTER TABLE {{ this }} DROP CONSTRAINT pk_artist_id');
            END IF;
        END
        $$;
        """
    ],
    post_hook = [
        "ALTER TABLE {{ this }} ADD CONSTRAINT pk_artist_id PRIMARY KEY (id)"
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

all_raw_artists as (
    select name from album_artists
    union all
    select name from track_artists
),

distinct_artists as (
    select distinct trim(name) as name
    from all_raw_artists
),

all_artists as (
    select name from distinct_artists
    union
    {% if is_incremental() %}
        select name from {{ this }}
    {% else %}
        select name from {{ ref('staging_artists') }}
    {% endif %}
),

deduplicated_artists as (
    select distinct name from all_artists
),

max_id as (
    {% if is_incremental() %}
        select coalesce(max(id), 0) as id from {{ this }}
    {% else %}
        select 0 as id
    {% endif %}
),

new_ids as (
    select
        row_number() over (order by name) + max_id.id as id,
        name
    from deduplicated_artists, max_id
)

select *
from new_ids
