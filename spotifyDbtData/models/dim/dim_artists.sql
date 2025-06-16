{% set sequence_name = 'artist_sk_seq' %}
{% set schema_name = target.schema %}
{% set database_name = target.database %}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS {{ database_name }}.{{ schema_name }}.{{ sequence_name }} START WITH 1;
{% endset %}

{% do run_query(sequence_query) %}

{{ config(
    materialized = 'incremental',
    unique_key = 'artist_id',
    incremental_strategy = 'merge'
) }}

SELECT
    {% if is_incremental() %}
        artist_sk,
    {% else %}
        NEXTVAL('{{ database_name }}.{{ schema_name }}.{{ sequence_name }}') AS artist_sk,
    {% endif %}

    artist_id,
    name AS artist_name

FROM {{ ref('all_artists_data') }}
WHERE artist_id IS NOT NULL
