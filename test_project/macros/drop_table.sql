{% macro drop_table(table_schema, table_name) %}
    {% set query %}
        drop table if exists {{ table_schema }}.{{ table_name }} cascade
    {% endset %}

    {%- if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation", "compile"] -%}

        {{log("Executing query: " ~ query ,true)}}
        {% do run_query(query) %}

    {% endif %}
{%- endmacro %}
