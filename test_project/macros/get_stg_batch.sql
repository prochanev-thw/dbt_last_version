-- get stg_batch
{% macro get_stg_batch(src_tab, src_pk, src_eff, src_cols, snp_tab, tgt_tab, mat_flag, distr_key) %}
  {%- if execute -%}

    {% set distr_key = distr_key|default(src_pk) %}

    {%- set query = stg_batch(
      src_tab = src_tab,
      src_pk = src_pk,
      src_eff = src_eff,
      src_cols = src_cols,
      snp_tab = snp_tab) -%}

    {{materialize_query(query = query,
                        tgt_tab = tgt_tab,
                        distr_key = distr_key,
                        mat_flag = mat_flag)}}
    {%- set msg = 'staging batch table created as ' ~ tgt_tab -%}
    {{return('--' ~ msg)}}
  {% endif %}
{%- endmacro %}