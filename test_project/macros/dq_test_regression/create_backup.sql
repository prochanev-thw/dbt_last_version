--create_backup
{% macro create_backup(schema_name, 
    table_name, 
    backup_schema='sandbox', 
    condition="") %}
  {% set query %}
    create table {{ backup_schema }}.{{ table_name }} as 
        select
            *
        from {{ schema_name }}.{{ table_name }}
        {% if '{{ condition }}' != "" %}
        where {{ condition }}
        {% endif %}
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% do run_query(query) %}
{%- endmacro %}
