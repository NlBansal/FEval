{% macro split_timestamp(column_name, alias) %}
    extract(year from {{ column_name }}::timestamp) as {{ alias }}_year,
    extract(month from {{ column_name }}::timestamp) as {{ alias }}_month,
    extract(day from {{ column_name }}::timestamp) as {{ alias }}_date,
    to_char({{ column_name }}::timestamp, 'HH24:MI:SS') as {{ alias }}_time
{% endmacro %}
