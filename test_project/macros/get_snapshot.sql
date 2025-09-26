-- get snapshot
{% macro get_snapshot(src_tab, src_hub, src_pk, src_nk, src_eff, src_cols, tgt_tab, incr_tab, distr_key, mat_flag, link_flag) %}
  {%- if execute -%}
    {%- if not (src_eff is iterable and src_eff is not string) -%}
      {%- set src_eff = [src_eff] -%}
    {%- endif -%}

    {%- if not (src_cols is iterable and src_cols is not string) -%}
      {%- set src_cols = [src_cols] -%}
    {%- endif -%}

    {% if link_flag is true %}
      {%- set join_col = src_cols[0] -%}
    {% else %}
      {%- set join_col = src_pk -%}
    {% endif %}

    {% if src_hub is not none and src_hub|string|length == 0 %}
      {% set src_hub = none %}
    {% endif %}

    {% if src_nk is not none and src_nk|string|length == 0 %}
      {% set src_nk = none %}
    {% endif %}

    {% if incr_tab is not none and incr_tab|length == 0 %}
      {% set incr_tab = none %}
    {% endif %}

    {% set link_flag = link_flag|default(false) %}

    {% set distr_key = distr_key|default(src_pk) %}

    {%- set sel_cols = dbtvault.expand_column_list(columns=[src_pk, src_cols]) -%}

    {%- set query = snapshot(src_tab = src_tab,
                            src_hub = src_hub,
                            src_pk = src_pk,
                            src_nk = src_nk,
                            src_eff = src_eff,
                            src_cols = src_cols,
                            incr_tab = incr_tab,
                            link_flag = link_flag) -%}

    {{materialize_query(query = query,
                        tgt_tab = tgt_tab,
                        distr_key = distr_key,
                        mat_flag = mat_flag)}}
    {%- set msg = 'snapshot table created as ' ~ tgt_tab -%}
    {{return('--' ~ msg)}}
  {% endif %}
{%- endmacro %}