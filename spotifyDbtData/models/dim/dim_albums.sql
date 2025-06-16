-- models/dim/dim_albums.sql

{% set sequence_name = 'album_sk_seq' %}
{% set schema_name = target.schema %}
{% set database_name = target.database %}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS {{ database_name }}.{{ schema_name }}.{{ sequence_name }} START WITH 1;
{% endset %}

{% do run_query(sequence_query) %}

{{ config(
    materialized = 'incremental',
    unique_key = 'album_id',
    incremental_strategy = 'merge'
) }}

SELECT
    {% if is_incremental() %}
        album_sk,  -- preserve existing surrogate key
    {% else %}
        NEXTVAL('{{ database_name }}.{{ schema_name }}.{{ sequence_name }}') AS album_sk,
    {% endif %}

    id AS album_id,
    name AS album_name

FROM {{ ref('staging_albums') }}
