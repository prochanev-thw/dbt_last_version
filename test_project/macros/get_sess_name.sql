-- get session name
{% macro get_sess_name(tab_name, session_suffix) %}
    {% set schema_const = 'stg' %}
    {% set sess_name = schema_const ~ '.' ~ tab_name ~ '_' ~  session_suffix %}
    {{return(sess_name)}}
{%- endmacro %}