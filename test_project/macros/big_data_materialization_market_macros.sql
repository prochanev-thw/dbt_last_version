-- NEW macro. Получаем ВСЕ партиции таблицы
{% macro get_partition_market(schema_name, table_name) %}
  {% set query %}

        SELECT
            substring(part.partitionrangestart from 2 for 10)::date as start_date,
            substring(part.partitionrangeend from 2 for 10)::date as end_date
        FROM pg_partitions part
        JOIN pg_class class ON part.partitiontablename = class.relname
        LEFT JOIN pg_namespace ns ON ns.oid = class.relnamespace
        WHERE
            part.schemaname = '{{ schema_name }}'
            AND part.tablename = '{{ table_name }}'
            AND pg_relation_size(class.oid) > 0
  {% endset %}

  {{log("Получаем ВСЕ партиции таблицы: " ~ schema_name ~ "." ~ table_name  ~ query ,true)}}

  {% set res = run_query(query) %}
  {{ return(res) }}

{%- endmacro %}


-- NEW macros. Макрос ЧЕКЕР, партиция есть в таблице и в ней есть данные
{% macro confirm_partition_existence_market(schema_name,
                                     table_name,
                                     part_start,
                                     include_size_check=false,
                                     include_partition_name=false) %}
  {% set query %}
        WITH target AS (
            SELECT
                substring(part.partitionrangestart from 2 for 10)::date AS start_date,
                substring(part.partitionrangeend from 2 for 10)::date AS end_date
                {%- if include_partition_name %}
                , part.partitiontablename AS partition_name
                {%- endif %}
            FROM pg_partitions part
            JOIN pg_class class ON part.partitiontablename = class.relname
            LEFT JOIN pg_namespace ns ON ns.oid = class.relnamespace
            WHERE
                part.schemaname = '{{ schema_name }}'
                AND part.tablename = '{{ table_name }}'
                {%- if include_size_check %}
                    AND pg_relation_size(class.oid) > 0
                {%- endif %}
                AND substring(part.partitionrangestart from 2 for 10)::date = '{{ part_start }}'::date
        )
        SELECT
            start_date,
            end_date
            {%- if include_partition_name %}
              , partition_name
            {%- endif %}
        FROM target
  {% endset %}

  {{ log("Проверка наличия партиции в: " ~ schema_name~"." ~ table_name ~ " " ~ batch_tmp_start ~ query, true) }}
  {% set result = run_query(query) %}


  {% if result %}
       {% if include_partition_name %}
          {{ return(result.rows[0].partition_name) }}
       {% else %}
          {{ return(result[0][0]) }}
       {% endif %}
  {% else %}
      {{ return(result) }}
  {% endif %}

{%- endmacro %}


-- NEW macros. ПОЛУЧАЕМ НОВЫЕ партиции батча которых нет в таргете
{% macro get_new_batch_partition_market(table_schema_name,
                                  table_name,
                                  batch_schema_name,
                                  batch_table_name) %}
  {% set query %}
        WITH
             batch as (
                SELECT
                    substring(part.partitionrangestart from 2 for 10)::date as start_date,
                    substring(part.partitionrangeend from 2 for 10)::date as end_date
                FROM pg_partitions part
                JOIN pg_class class ON part.partitiontablename = class.relname
                LEFT JOIN pg_namespace ns ON ns.oid = class.relnamespace
                WHERE
                    part.schemaname = '{{ batch_schema_name }}'
                    AND part.tablename = '{{ batch_table_name }}'
                    AND pg_relation_size(class.oid) > 0
             ),
            target as (
                SELECT
                    substring(part.partitionrangestart from 2 for 10)::date as start_date,
                    substring(part.partitionrangeend from 2 for 10)::date as end_date
                FROM pg_partitions part
                JOIN pg_class class ON part.partitiontablename = class.relname
                LEFT JOIN pg_namespace ns ON ns.oid = class.relnamespace
                WHERE
                    part.schemaname = '{{ table_schema_name }}'
                    AND part.tablename = '{{ table_name }}'
                    AND pg_relation_size(class.oid) > 0
             )
        SELECT
            b.start_date,
            b.end_date
        FROM batch as b
        LEFT JOIN target as t
             ON b.start_date = t.start_date
            AND b.end_date= t.end_date
        WHERE t.start_date is null;
  {% endset %}

  {{log("Получаем НОВЫЕ партиции батча которых нет в таргете: " ~ query ,true)}}

  {% set new_batch_partition = run_query(query) %}
  {{log("New_batch_partition: " ~ new_batch_partition ,true)}}
  {% for raw in new_batch_partition %}
      {{log("New batch partititon: " ~ "start " ~ raw[0] ~ " end "~ raw[1] ,true)}}
  {% endfor %}
  {{log("----- " ,true)}}

  {{ return(new_batch_partition) }}

{%- endmacro %}

-- NEW macros. УДАЛЕНИЕ tmp таблицы БАЧА
{% macro drop_batch_month_market(batch_tmp_full_name) %}
      {%for i in range(0,5) %}

  {% set query %}
    DROP TABLE IF EXISTS {{ batch_tmp_full_name }}{{i}};
  {% endset %}

  {{log("УДАЛЕНИЕ tmp таблицы БАЧА: " ~ "batch_tmp_full_name " ~ query ,true)}}

  {% do run_query(query) %}
      {% endfor %}
}
{%- endmacro %}


  -- NEW macros. ПОЛУЧЕНИЕ колонок таблицы
  {% macro get_columns_market(table_name, schema_name) %}
  {% set query %}
    select column_name
    from information_schema.columns
    where table_schema = '{{schema_name}}'
    and table_name  = '{{table_name}}'
    order by ordinal_position ;
  {% endset %}

  {{log("Executing query get_columns: " ~ query ,true)}}

  {% set res = run_query(query) %}
  {{ return(res) }}

{%- endmacro %}


-- NEW macros. СОЗДАНИЕ tmp БАЧА партиции для EXCHANGE PARTITION с таргетом
-- create table
{% macro create_batch_month_market(batch_tmp_full_name,
                             batch_schema_name,
                             batch_table_name,
                             table_schema_name,
                             table_name,
                             table_name_partition,
                             table_pk,
                             distributed_by,
                             batch_tmp_start,
                             all_cols,
                             date_col,idx) %}

  {% if execute %}
  {% set cols = all_cols.columns[0].values() %}
  {% else %}
  {% set cols = [] %}
  {% endif %}

  {%- if not (date_col and date_col.strip()) %}
    {%- set date_col = 'datetime' -%}
  {%- endif -%}
  {% set query %}
        create table {{ batch_tmp_full_name }}{{idx}}
        WITH (appendonly=true,
              blocksize=32768,
              compresstype=zstd,
              compresslevel=4,
              orientation=column) as
        with
            batch as (
                select *
                from {{ batch_schema_name }}.{{ batch_table_name }}  src
                where (src.{{ date_col }} >=  date_trunc('MONTH','{{ batch_tmp_start }}'::date) - Interval '{{idx}} month'
                  and src.{{ date_col }} < date_trunc('MONTH','{{ batch_tmp_start }}'::date) - Interval '{{idx-1}} month')
                )
        select
            {% for col in cols[:-1] %}
                coalesce(src.{{ col }}, tgt.{{ col }}) as {{ col }},
            {% endfor %}
            coalesce(src.{{ cols[-1] }}, tgt.{{ cols[-1] }}) as {{ cols[-1] }}
        from {{ table_schema_name }}.{{ table_name_partition }}  tgt
        full join batch src on
        {% if table_pk|length > 1 %}
            {% for pk in table_pk[:-1] %}
                src.{{ pk }} = tgt.{{ pk }} and
            {% endfor %}
        {% endif %}
         src.{{ table_pk[-1] }} = tgt.{{ table_pk[-1]  }}
        distributed by ({{ distributed_by }});
  {% endset %}

  {{log("Executing query FULL JOIN: " ~ query ,true)}}

  {% do run_query(query) %}

{%- endmacro %}


-- NEW macros. EXCHANGE PARTITION
{% macro exchange_partition_market(table_schema_name, table_name, batch_tmp_full_name,formatted_date,idx) %}

  {% set query %}
    ALTER TABLE {{ table_schema_name }}.{{ table_name }} EXCHANGE PARTITION FOR ('{{ formatted_date }}') WITH TABLE {{ batch_tmp_full_name }}{{idx}};
  {% endset %}
  {{log("Executing query: " ~ query ,true)}}

  {% do run_query(query) %}
        {{log("Executing query: " ~ idx ,true)}}


{%- endmacro %}


-- NEW macros. Запуск ХУКОВ.
-- hook_type: "pre" | "post",
-- inside_transaction: если true, то с begin,commit,rollback. SET хуки всегда первыми и вне транзакции.
{% macro run_hooks_query_market(hook_type, hooks_query=None, inside_transaction=True) %}
    {{ log("----- ", true) }}

    {% if hooks_query is none or hooks_query == [] %}
        {{ log("Переменная c HOOK's, типа " ~ hook_type ~ " НЕ ОБЪЯВЛЕНА в конфиге или список хуков ПУСТОЙ. Выполнение " ~ hook_type ~ " пропускается", true) }}
        {{ log("----- ", true) }}
    {% else %}
        {{ log("HOOK's к выполнению, тип " ~ hook_type ~ " hooks: " ~ hooks_query, true) }}

        {{ log("HOOK этап_1. Запуск SET хуков ВНЕ транзакции", true) }}
        {% for hook in hooks_query %}
            {% if hook.upper().startswith('SET') %}
                {{ log("Запуск SET хука: " ~ hook, true) }}
                {{ adapter.execute(hook) }}
            {% endif %}
        {% endfor %}

        {% if inside_transaction %}
            {{ log("HOOK этап_2. Запуск оставшихся хуков ВНУТРИ транзакции", true) }}
            {% set transaction_start = adapter.execute("BEGIN") %}

            {% set transaction_success = True %}
            {% for hook in hooks_query %}
                {% if not hook.upper().startswith('SET') %}
                    {% if transaction_success %}
                        -- TODO: дополнить другими проверками хук запросы
                        {% if hook.strip().lower().startswith('drop table') %}
                            {% set table_name = hook.split(' ')[-1] %}
                            {% set table_exists_query = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'sandbox' AND table_name = '" ~ table_name.split('.')[-1] ~ "'" %}
                            {% set table_exists = run_query(table_exists_query) %}
                            {% if table_exists %}
                                {{ log("Запуск хука DROP TABLE ВНУТРИ транзакции: " ~ hook, true) }}
                                {% set result = adapter.execute(hook) %}
                            {% else %}
                                {{ log("Таблица не существует, пропуск команды DROP TABLE: " ~ table_name, true) }}
                            {% endif %}
                        {% else %}
                            {{ log("Запуск non-SET хука ВНУТРИ транзакции: " ~ hook, true) }}
                            {% set result = adapter.execute(hook) %}
                        {% endif %}

                        {% if not result %}
                            {{ log("Ошибка при выполнении хука: " ~ hook ~ ". ОТКАТ транзакции.", true) }}
                            {{ adapter.execute("ROLLBACK") }}
                            {% set transaction_success = False %}
                        {% endif %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {% if transaction_success %}
                {{ adapter.execute("COMMIT") }}
            {% endif %}
        {% else %}
            {{ log("HOOK этап_2. Запуск оставшихся хуков ВНЕ транзакции", true) }}
            {% for hook in hooks_query %}
                {% if not hook.upper().startswith('SET') %}
                    {% if hook.strip().lower().startswith('drop table') %}
                        {% set table_name = hook.split(' ')[-1] %}
                        {% set table_exists_query = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'sandbox' AND table_name = '" ~ table_name.split('.')[-1] ~ "'" %}
                        {% set table_exists = run_query(table_exists_query) %}
                        {% if table_exists %}
                            {{ log("Запуск хука DROP TABLE ВНЕ транзакции: " ~ hook, true) }}
                            {{ adapter.execute(hook) }}
                        {% else %}
                            {{ log("Таблица не существует, пропуск команды DROP TABLE: " ~ table_name, true) }}
                        {% endif %}
                    {% else %}
                        {{ log("Запуск non-SET хука ВНЕ транзакции: " ~ hook, true) }}
                        {{ adapter.execute(hook) }}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {{ log("----- ", true) }}
{% endmacro %}
