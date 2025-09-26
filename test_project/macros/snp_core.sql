-- generate core snapshot
{% macro snp_core(src_model, src_pk, src_eff, src_payload, clause=none) %}

  {% if src_eff is iterable and src_eff is not string %}
    {% set src_eff = src_eff[0] %}
  {% endif %}

  {% if not (src_payload is iterable and src_payload is not string) %}
    {% set src_payload = [src_payload] %}
  {% endif %}

  {%- set sel_cols = dbtvault.expand_column_list(columns=[src_pk, src_payload]) | unique | list -%}

  {% set query %}
    select {{ dbtvault.prefix(sel_cols, 'a') }}
    from (
      select s.*,
        row_number() over (partition by {{ dbtvault.prefix([src_pk], 's') }} order by s.{{ src_eff }} desc) as rn
      from {{src_model}} s) a
    where a.rn = 1{%- if clause is not none %} and {{clause}}{%- endif %}
  {% endset %}

  {{return(query)}}
{%- endmacro %}