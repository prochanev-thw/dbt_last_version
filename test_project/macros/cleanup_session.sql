-- cleanup session
{% macro cleanup_session(session_suffix) %}
  {%- if execute -%}
    {% call statement('list_tabs', fetch_result=True) -%}
        select table_schema||'.'||table_name as tab,
               case when table_type = 'BASE TABLE' then 1 else 0 end tab_flg
        from information_schema.tables where upper(table_name) like upper('%{{session_suffix}}%')
    {%- endcall %}

    {% set res = load_result('list_tabs') %}
    {% set cnt = res.response.rows_affected %}
    {% set tabs = res.table %}
    {{log("found: " ~ cnt ~ ' objects',true)}}

    {%- for tab in tabs -%}
        {%- if tab[1] == 1 -%}
        {% set query = 'drop table if exists ' ~ tab[0] %}
        {%- else %}
        {% set query = 'drop view if exists ' ~ tab[0] %}
        {%- endif %}
        {{log("processing: " ~ query ,true)}}
        {{run_query(query)}}
    {% endfor -%}

    {%- set msg = 'session cleaned up' -%}
    {{return('--' ~ msg)}}
  {% endif %}
{%- endmacro %}