-- create table
{% macro create_batch_month(batch_table_month, batch_table, schema_name, table_pk, distributed_by, part, part_date,
 all_cols, date_col, full_join_without_filter_d) %}


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
                   {% if not full_join_without_filter_d %}
                       or (case when sys_change_operation = 'D' then {{ date_col }} is null
                                else False end)
                   {% endif %}
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
                 {% if batch_table_month == "dm_batch.cuscom_notifications_month" %}
                   and coalesce(tgt.status, 'xexe')                           = coalesce(src.status, 'xexe')
                   and coalesce(tgt.magnit_id, 'xexe')                        = coalesce(src.magnit_id, 'xexe')
                   and coalesce(tgt.title, 'xexe')                            = coalesce(src.title, 'xexe')
                   and coalesce(tgt."text", 'xexe')                           = coalesce(src."text", 'xexe')
                   and coalesce(tgt."type", 'xexe')                           = coalesce(src."type", 'xexe')
                   and coalesce(tgt.created_at, '1970-01-01'::timestamp)      = coalesce(src.created_at, '1970-01-01'::timestamp)
                   and coalesce(tgt.expire_at, '1970-01-01'::timestamp)       = coalesce(src.expire_at, '1970-01-01'::timestamp)
                   and coalesce(tgt.readed_at, '1970-01-01'::timestamp)       = coalesce(src.readed_at, '1970-01-01'::timestamp)
                 {% endif %}

                {% if not full_join_without_filter_d %}
                    where src.sys_change_operation != 'D' or src.sys_change_operation is null
                {% endif %}
                distributed by ({{ distributed_by }});
  {% endset %}

  {{log("Executing query: " ~ query ,true)}}

  {% do run_query(query) %}

{%- endmacro %}
