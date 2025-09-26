-- exchange partition
{% macro exchange_partition(schema_name, table_name, batch_table_month, part) %}
  {% set query %}
    ALTER TABLE {{ schema_name }}.{{ table_name }} EXCHANGE PARTITION FOR ('{{ part }}') WITH TABLE {{ batch_table_month }};
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% do run_query(query) %}

{%- endmacro %}