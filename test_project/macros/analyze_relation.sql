{% macro analyze_relation() %}
{# сбор статистики по модели
  Если модель имеет материализацию table/incremental_reload, то она аналаизируется целиком, независимо от наличия партиций.
  Если модель имеет материализацию incremental и не содержит партиций, то тоже целиком.
  Если модель имеет материализацию incremental и содержит партиции, то ищем в конфиге метаданных модели атрибуты source_batch или source_clause,
  собираем по ним данные об изменившихся партициях и анализируем только их. Остальные кейсы (другие материализации, отсутствие атрибутов) - игнорируем
#}
  {% if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] and load_relation(this) is not none %}
    {% if model.config.materialized in ["table", "incremental_reload"] or (model.config.materialized == "incremental"
      and model.config.fields_string is not defined) %}
      {% set query %}
      analyze {{this}}
      {% endset %}
      {% if model.config.stats_weekly is not defined %}
        {{log("analyzing table: " ~ this, true)}}
        {% do run_query(query) %}
      {% elif weekday_sensor(model.config.stats_weekly) %}
        {{log("analyzing table: " ~ this, true)}}
        {% do run_query(query) %}
      {% else %}
        {{log("today is not " ~ model.config.stats_weekly ~ ", skipping stats", true)}}
      {% endif %}
    {% elif model.config.materialized == "incremental"
      and model.config.fields_string is defined
      and (model.config.source_clause is defined or model.config.source_batch is defined) %}
      {% set prt_type = get_partition_type(this) %}
      {% set prt_col_type = get_partition_column_type(this) %}
      {% if model.config.source_clause is not defined %}
        {% if model.config.source_column is not defined %}
          {% set prt_col = get_partition_column(this) %}
        {% else %}
          {% set prt_col = model.config.source_column %}
        {% endif %}
        {% set src_clause = get_batch_src_clause(model.config.source_batch, prt_col, prt_col_type) %}
      {% else %} 
        {% set src_clause = model.config.source_clause %}
      {% endif %}
      {{ gather_partition_stats_raw(src_clause, this, prt_type, prt_col_type) }}
    {% endif %}
  {% endif %}
{% endmacro %}