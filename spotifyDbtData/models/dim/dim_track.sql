{% set sequence_name = 'track_sk_seq' %}
{% set schema_name = target.schema %}
{% set database_name = target.database %}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS {{ database_name }}.{{ schema_name }}.{{ sequence_name }} START WITH 1;
{% endset %}

{% do run_query(sequence_query) %}
{{ config(
    materialized = 'incremental',
    unique_key = 'track_sk',
    incremental_strategy = 'merge',
    indexes = [
        {"columns": ["track_id"]}
    ]
) }}

select 
    {% if is_incremental() %}
        track_sk,
    {% else %}
        NEXTVAL('{{ database_name }}.{{ schema_name }}.{{ sequence_name }}') AS track_sk,
    {% endif %}

    id as track_id,
    name as track_name
from {{ ref('staging_tracks') }}