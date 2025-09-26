{% macro get_day_of_month(date_string) %}
    {% set sql %}
        select extract(day from cast('{{ date_string }}' as date));
    {% endset %}
    {% set res = run_query(sql).columns[0].values()[0] %}
    {{log("Day of month: " ~ res ,true)}}
    {{ return (res) }}
{% endmacro %}