{% macro get_day_of_week(date_string) %}
    {% set sql %}
        select extract(dow from cast('{{ date_string }}' as date));
    {% endset %}
    {% set res = run_query(sql).columns[0].values()[0] %}
    {{log("Day of week: " ~ res ,true)}}
    {{ return (res) }}
{% endmacro %}