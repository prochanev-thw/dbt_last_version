-- generate stg_batch
{% macro stg_batch(src_model, src_pk, src_eff, src_payload, base_tab=none, tgt_pk=none, tgt_eff=none, full_stg_batch=none, clause=none) %}

  {%- set sel_cols = dbtvault.expand_column_list(columns=[src_pk, src_eff, src_payload]) | unique | list -%}
  {%- set tech_cols = dbtvault.expand_column_list(columns=[src_eff, src_payload]) | unique | list -%}

  {% if tgt_pk is none %}
    {% set tgt_pk = src_pk %}
  {%- endif %}

  {% if tgt_eff is none %}
    {% set tgt_eff = src_eff %}
  {%- endif %}

  {% set conditions = [] %}
  {%- if base_tab is not none %}
    {%- if full_stg_batch is none %}
      {% do conditions.append('(b.' ~ src_eff ~ ' > c.' ~ tgt_eff ~ ' or c.' ~ tgt_eff ~ ' is null)') %}
    {%- else %}
      {% do conditions.append('(b.' ~ src_eff ~ ' >= c.' ~ tgt_eff ~ ' or c.' ~ tgt_eff ~ ' is null)') %}
    {%- endif %}
  {%- endif %}
  {%- if clause is not none %}
    {% do conditions.append(clause) %}
  {%- endif %}

  {% set query %}
    select {{ dbtvault.prefix(sel_cols, 'b') }}
    from ({{snp_core(src_model = src_model, src_pk = src_pk, src_eff = src_eff, src_payload = tech_cols)}}) b
    {%- if base_tab is not none %}
    left join {{base_tab}} c on c.{{tgt_pk}} = b.{{src_pk}}
    {%- endif %}
    {%- if conditions|length > 0 %}
    where {{ conditions | join(' and ') }}
    {%- endif %}
  {% endset %}

  {{return(query)}}
{%- endmacro %}