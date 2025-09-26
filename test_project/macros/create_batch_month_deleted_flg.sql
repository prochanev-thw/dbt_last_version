-- create table
{% macro create_batch_month_deleted_flg(batch_table_month, batch_table, schema_name, table_pk, distributed_by, part, part_date,
 all_cols, date_col) %}


  {% if execute %}
  {% set cols = all_cols.columns[0].values() %}
  {% else %}
  {% set cols = [] %}
  {% endif %}

  {%- if not (date_col and date_col.strip()) %}
    {%- set date_col = 'datetime' -%}
  {%- endif -%}

  {% set query %}
    create table {{ batch_table_month }}
    WITH (appendonly=true,
          blocksize=32768,
          compresstype=zstd,
          compresslevel=4,
          orientation=column) as
    with batch as (select * from {{ batch_table }} src
                   where ({{ date_col }} >= '{{ part_date }}'::date
                          and src.{{ date_col }} < '{{ part_date }}'::date + interval '1 months')
                   or (case when deleted_flg = 1 then {{ date_col }} is null
                            else False end)
                   )
                select
                {% for col in cols[:-1] %}
                    coalesce(src.{{ col }}, tgt.{{ col }}) as {{ col }},
                {% endfor %}
                coalesce(src.{{ cols[-1] }}, tgt.{{ cols[-1] }}) as {{ cols[-1] }}
                from {{ schema_name }}.{{ part }}  tgt
                full join batch src on
                {% if table_pk|length > 1 %}
                    {% for pk in table_pk[:-1] %}
                        src.{{ pk }} = tgt.{{ pk }} and
                    {% endfor %}
                {% endif %}
                 src.{{ table_pk[-1] }} = tgt.{{ table_pk[-1]  }}
                 {% if batch_table_month == "dm.bonus_all_optimized_month" %}
                    and coalesce(src.bonus_wo_pk::text,'xexe') = coalesce(tgt.bonus_wo_pk::text,'xexe')
                    and coalesce(src.bonus_pk::text,'xexe') = coalesce(tgt.bonus_pk::text,'xexe')
                    and coalesce(src.bonus_id,-999) = coalesce(tgt.bonus_id,-999)
                 {% endif %}
                where src.deleted_flg != 1 or src.deleted_flg is null
                distributed by ({{ distributed_by }});
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% do run_query(query) %}

{%- endmacro %}
