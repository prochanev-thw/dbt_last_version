-- get partitions from table
{% macro get_partition(table_name, schema_name, batch_table, date_col) %}
  {% set query %}
    select partitiontablename, d.{{ date_col }}
    from pg_catalog.pg_partitions p
    join (
          select distinct date_trunc('month', {{ date_col }})::date  as {{ date_col }}
          from {{ batch_table }}
          order by date_trunc('month', {{ date_col }})::date
          ) d
    on substring(p.partitionrangestart FROM 2 FOR 11) ::date = d.{{ date_col }}
    where tablename = '{{ table_name }}'
          and schemaname = '{{ schema_name }}'
    order by d.{{ date_col }};
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% set res = run_query(query) %}
  {{ return(res) }}

{%- endmacro %}