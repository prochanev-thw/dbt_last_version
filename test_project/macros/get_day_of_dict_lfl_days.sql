{% macro get_day_of_dict_lfl_days(date_string, date_interval) %}
 {% set sql %}
        select prev_day_id_for_day
        from dict.lfl_days
        WHERE day_id = (date_trunc('{{ date_string }}', CURRENT_DATE)) - interval '{{ date_interval }}'
    {% endset %}
    {% set result = run_query(sql) %}
    {% if execute %}
        {{ log(msg, True) }}
        {% set res = result.columns[0].values()[0] %}
    {% else %}
        {% set res = 0 %}
    {% endif %}
    {{ return(res) }}
{% endmacro %}