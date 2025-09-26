{% macro truncate_partitions() %}
{# очистка партиций по данным батча/выражению
  Если модель имеет материализацию incremental и содержит партиции, то ищем в конфиге метаданных модели атрибуты source_batch или source_clause,
  собираем по ним данные об изменившихся партициях и очищаем только их. Остальные кейсы (другие материализации, отсутствие атрибутов) - игнорируем
#}
  {% if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] and load_relation(this) is not none %}
    {% if model.config.materialized == "incremental" and model.config.fields_string is defined
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
      {{ add_missing_partitions(src_clause, this, prt_type, prt_col_type) }}
      {{ truncate_partition_raw(src_clause, this, prt_type, prt_col_type) }}
    {% endif %}
  {% endif %}
{% endmacro %}