{% macro should_run_first_saturday_from_15(dm_date_text, inc_date_text) %}
{#
    Возвращает True, если:
      • dm_date_text   – первая суббота, начиная с 15-го числа месяца
      • inc_date_text  – любое число ≥ 15-го
    Ожидаемые условия для запуска обогащения батча.
            -дата запуска это первая СБ после 15-го (mission 15го уже пришли, за весь предыдущий месяц)
            -есть дневной инкремент sales_header с датой >15го
    Если в первую СБ от 15го условия не совпадут,
            !!! уйдет на след. месяц.и пропустит миссии пред месяца !!!
#}

    {# --- валидация наличия дат --- #}
    {% if dm_date_text is none or dm_date_text | trim == '' %}
        {{ return(false) }}
    {% endif %}
    {% if inc_date_text is none or inc_date_text | trim == '' %}
        {{ return(false) }}
    {% endif %}

    {% set run_dt = modules.datetime.datetime.strptime(dm_date_text,  '%Y-%m-%d') %}
    {% set inc_dt = modules.datetime.datetime.strptime(inc_date_text, '%Y-%m-%d') %}

    {# --- если инкремент до 15-го числа – сразу false --- #}
    {% if inc_dt.day < 15 %}
        {{ return(false) }}
    {% endif %}

    {# --- проверка «run_dt — первая суббота ≥ 15-го» --- #}
    {% set fifteenth = run_dt.replace(day=15) %}
    {% for offset in range(0, 7) %}
        {% set cand = fifteenth + modules.datetime.timedelta(days=offset) %}
        {% if cand.weekday() == 5 %}
            {{ return(run_dt.date() == cand.date()) }}
        {% endif %}
    {% endfor %}

    {{ return(false) }}
{% endmacro %}