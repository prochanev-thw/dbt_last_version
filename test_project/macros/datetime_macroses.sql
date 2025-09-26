{% macro weekday_sensor(weekday) %}
{# Макрос возвращает true, если сегодня день недели, указанный в параметре weekday
  и false, если любой другой. Время берется московское
  Допускаются сокращения, типа sat/sun/mond и т.д.
#}
  {% set currdate = run_started_at.astimezone(modules.pytz.timezone("Europe/Moscow")) %}
  {% set currday = currdate.strftime("%a") | lower%}
  {% if weekday.lower().startswith(currday) %}
    {{ log("thank god it's " ~ weekday ~ "!", true) }}
    {% do return(true) %}
  {% else %}
    {% do return(false) %}
  {% endif %}
{% endmacro %}

{% macro string_to_date(date_string) %}
{# очищаем строку с цифрами от возможных разделителей и интерпретируем её первые 8 символов как дату YYYYMMDD #}
  {% set prepared_dt = modules.re.sub("[^0-9]", "", date_string)[:8] %}
  {% set dt = modules.datetime.datetime.strptime(prepared_dt,"%Y%m%d") %}
  {{ return(dt) }}
{% endmacro %}