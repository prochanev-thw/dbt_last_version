-- get increment
{% macro get_increment(src_pk, src_tabs, process_code, tgt_tab, mat_flag) %}
  {%- if execute -%}
    {%- if not (src_tabs is iterable and src_tabs is not string) -%}
      {%- set src_tabs = [src_tabs] -%}
    {%- endif -%}

    {% if process_code is not none and process_code|string|length == 0 %}
      {% set process_code = none %}
    {% endif %}

    {% set query %}
        {%- for src_tab in src_tabs -%}
          select distinct {{src_pk}} from {{src_tab}}
          {% if process_code is not none %}
          where process_code = '{{process_code}}'::timestamp
          {% endif %}
          {% if not loop.last %}
          union
          {% endif %}
        {% endfor -%}
    {% endset %}

    {{materialize_query(query = query,
                        tgt_tab = tgt_tab,
                        distr_key = src_pk,
                        mat_flag = mat_flag)}}
    {%- set msg = 'increment table created as ' ~ tgt_tab -%}
    {{return('--' ~ msg)}}
  {% endif %}
{%- endmacro %}