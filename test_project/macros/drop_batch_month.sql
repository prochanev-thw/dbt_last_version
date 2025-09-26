-- drop table if exist
{% macro drop_batch_month(batch_table_month) %}
  {% set query %}
    DROP TABLE IF EXISTS {{ batch_table_month }};
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% do run_query(query) %}
{%- endmacro %}