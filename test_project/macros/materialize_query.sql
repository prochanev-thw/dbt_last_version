-- materialize_query
{% macro materialize_query(query, tgt_tab, distr_key, mat_flag) %}
  {%- if execute -%}
    {% set mat_flag = mat_flag|default(true) %}

    {% if distr_key is not none and distr_key|length == 0 %}
      {% set distr_clause = 'distributed randomly' %}
    {% else %}
      {% set distr_clause = 'distributed by (' ~ distr_key ~ ')'%}
    {% endif %}

    {% if mat_flag is true %}
      {% set type_clause = 'table' %}
      {% set drop_clause = 'drop table if exists ' ~ tgt_tab %}
    {% else %}
      {% set type_clause = 'view' %}
      {% set distr_clause = ''%}
      {% set drop_clause = 'drop view if exists ' ~ tgt_tab %}
    {% endif %}

    {% set clause %}
        create {{type_clause}} {{tgt_tab}} as {{query}} {{distr_clause}}
    {% endset %}

    {{log("Executing query: " ~ clause ,true)}}
    {{run_query(drop_clause)}}
    {{run_query(clause)}}
    {% call statement('main', fetch_result=False) -%}
      COMMIT;
    {%- endcall %}
    {%- set msg = 'query materialized as ' ~ tgt_tab -%}
    {{return('--' ~ msg)}}
  {% endif %}
{%- endmacro %}