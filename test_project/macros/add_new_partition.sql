--получаем список партиций которые есть в батче, но нет в целевой таблице
{% macro get_batch_partition(table_name, schema_name, batch_table, date_col, partition_period) %}
  {% set query %}
    WITH target as (SELECT partitionrangestart, partitionrangeend
               FROM pg_catalog.pg_partitions
               WHERE schemaname = '{{ schema_name }}' and tablename = '{{ table_name }}'),
          batch as (select distinct date_trunc('{{ partition_period }}', {{ date_col }} ::timestamp)::date as partition_date
                from {{ batch_table }}
                where {{ date_col }}::timestamp is not null
                order by partition_date)
          SELECT b.partition_date
          FROM batch b
          LEFT JOIN target t
          ON b.partition_date >= substring(t.partitionrangestart,2,10)::date
          AND b.partition_date < substring(t.partitionrangeend,2,10)::date
          WHERE t.partitionrangestart is null;
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% set res = run_query(query) %}
  {{ return(res) }}

{%- endmacro %}

--получаем завершающий отрезок партиции
{% macro batch_partition_end_macro(batch_partition, partition_period) %}
  {% set query %}
      SELECT '{{ batch_partition }}'::timestamp + interval '1' {{ partition_period }}
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% set res = run_query(query) %}
  {{ return(res) }}

{%- endmacro %}

--добавляем новые партиции в целевую таблицу
{% macro add_new_partition(table_name, schema_name, batch_table, batch_partition, batch_partition_end, partition_period) %}
  {% set query %}
    alter table {{ schema_name }} . {{ table_name }} add partition start ('{{ batch_partition }}'::timestamp) inclusive end ('{{ batch_partition_end }}'::timestamp) exclusive
   WITH (appendonly=true, blocksize=32768, compresstype=zstd, compresslevel=4, orientation=column);
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% set res = run_query(query) %}
  {{ return(res) }}

{%- endmacro %}



