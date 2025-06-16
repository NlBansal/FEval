{% set sequence_name = 'album_sk_seq' %}
{% set schema_name = target.schema %}
{% set database_name = target.database %}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS {{ database_name }}.{{ schema_name }}.{{ sequence_name }} START WITH 1;
{% endset %}

{% do run_query(sequence_query) %}

{{ config(
    materialized = 'incremental',
    unique_key = 'album_sk',
    incremental_strategy = 'merge',
    indexes = [
      {"columns": ["album_id"]}
    ]
) }}

SELECT
    {% if is_incremental() %}
        album_sk,  
    {% else %}
        NEXTVAL('{{ database_name }}.{{ schema_name }}.{{ sequence_name }}') AS album_sk,
    {% endif %}

    id AS album_id,
    name AS album_name,
    release_date AS album_release_date,
    total_tracks AS album_total_tracks,
    popularity AS album_popularity
FROM {{ ref('staging_albums') }}
