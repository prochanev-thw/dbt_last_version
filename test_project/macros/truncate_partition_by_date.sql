{% macro truncate_partition_by_date(table, date) %}

  {% set query %}
   alter table {{ table }} truncate partition for ('{{ date }}')
  {% endset %}

  {% do run_query(query) %}

{% endmacro %}
