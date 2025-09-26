-- increment
{% macro increment(src_pk, src_tabs, process_code = none, depth = none) %}
  {%- if not (src_tabs is iterable and src_tabs is not string) -%}
    {%- set src_tabs = [src_tabs] -%}
  {%- endif -%}

  {%- set query -%}
  select distinct a.{{src_pk}}
  from (
  {%- for src_tab in src_tabs %}
  select {{src_pk}}
  from {{src_tab}}
  {%- if process_code is not none %}
  where process_code::date{%- if depth is none %} = '{{process_code}}'::date{%- else %} between '{{process_code}}'::date - interval '{{depth}}' day and '{{process_code}}'::date{%- endif -%}
  {% endif %}
  {% if not loop.last %}union all{% endif %}
  {%- endfor -%}) a
  {%- endset -%}
  {{return(query)}}
{%- endmacro %}