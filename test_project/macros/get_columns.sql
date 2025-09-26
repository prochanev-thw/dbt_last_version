-- get columns
{% macro get_columns(table_name, schema_name) %}
  {% set query %}
    select column_name
    from information_schema.columns
    where table_schema = '{{schema_name}}'
    and table_name  = '{{table_name}}'
    order by ordinal_position ;
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% set res = run_query(query) %}
  {{ return(res) }}

{%- endmacro %}