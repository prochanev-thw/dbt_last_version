-- lock_relation
{% macro lock_relation() %}
{# функция блокирует модель в момент выполнения и только в том случае, если она существует #}
  {%- if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] and load_relation(this) is not none -%}
    {% set query %}
      lock table {{this}}
    {% endset %}
    {% do run_query(query) %}
  {%- endif -%}
{%- endmacro %}