-- generate snapshot
{% macro snapshot(src_tab, src_pk, src_eff, src_cols, src_hub=none, src_nk=none, incr_tab=none, clause=none, link_flag=False, join_key=none, incr_key=none) %}
  {% if src_eff is iterable and src_eff is not string %}
    {% set src_eff = src_eff[0] %}
  {% endif %}

  {% if not (src_cols is iterable and src_cols is not string) %}
    {% set src_cols = [src_cols] %}
  {% endif %}

  {% set internal_cols = src_cols %}

  {% if src_hub is none %}
    {% set src_nk = none %}
  {% else %}
    {% if link_flag is true %}
      {% set hub_key = src_cols[0] %}
    {% else %}
      {% set hub_key = src_pk %}
    {% endif %}
    {% if join_key is none %}
      {% set join_key = hub_key %}
    {% else %}
      {% set internal_cols = src_cols.remove(hub_key) %}
      {% set src_nk = dbtvault.expand_column_list(columns=[src_nk, hub_key]) | unique | list %}
    {% endif %}
  {% endif %}

  {% if incr_key is none %}
    {% set incr_key = src_pk %}
  {%- endif %}

  {%- set tech_cols = dbtvault.expand_column_list(columns=[src_pk, src_eff, internal_cols, join_key]) | unique | list -%}
  {%- set src_cols = dbtvault.expand_column_list(columns=[src_pk, src_nk, src_cols]) | unique | list -%}

  {% set query %}
  select {{ dbtvault.prefix(src_cols, 'c') }}
  from (
    select {{ dbtvault.prefix(tech_cols, 'b') }}{% if src_nk is not none %}, {{ dbtvault.prefix([src_nk], 'h') }}{% endif %}
    from ({{snp_core(src_model = src_tab,src_pk = src_pk,src_eff = src_eff,src_payload = tech_cols)}}) b
  {%- if src_hub is not none %}
  join {{src_hub}} h on h.{{hub_key}} = b.{{join_key}}
  {%- endif %}
  {%- if incr_tab is not none %}
  {%- if ' ' in incr_tab  or '.' in incr_tab %}
  join ({{incr_tab}}) i on {{ dbtvault.multikey(incr_key, prefix=['i','b'], condition='=') }}
  {% else %}
  join {{incr_tab}} i on {{ dbtvault.multikey(incr_key, prefix=['i','b'], condition='=') }}
  {%- endif %}
  {%- endif %}) c
  {%- if clause is not none %} where {{clause}}{%- endif %}
  {% endset %}

  {{return(query)}}
{%- endmacro %}