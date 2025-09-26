{% macro exec_sql_list(sql_list) %}
{%- if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] -%}
  {%- for q in sql_list %}
    {{ log("[exec_sql_list] Executing: " ~ q, info=True) }}
    {{ run_query(q) }}
  {%- endfor %}
{% endif %}
{% endmacro %}
