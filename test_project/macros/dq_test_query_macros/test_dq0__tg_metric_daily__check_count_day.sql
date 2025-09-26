{% macro test_dq0__tg_metric_daily__check_count_day(model) %}
{% set model = '' %}

    SELECT
        frmt_eng,
        metric,
        count(dt) AS count_date,
        case
            when count(distinct dt) = 14
            then 1
            else 0
        end date_check_result,
        14 - count(distinct dt) missed_days,
        '{{ var("dm_date") }}'::date as dq_date_test,
        current_timestamp as dq_load_datetime
    FROM {{ ref('tg_metric_daily_format') }}
    WHERE frmt_eng != 'Мpharm'
    GROUP BY frmt_eng, metric
    UNION ALL
    SELECT
        'Total'::varchar frmt,
        metric,
        count(dt) AS count_date,
        case
            when count(distinct dt) = 14
            then 1
            else 0
        end date_check,
        14 - count(distinct dt) missed_days,
        '{{ var("dm_date") }}'::date as dq_date_test,
        current_timestamp as dq_load_datetime
    FROM {{ ref('tg_metric_daily') }}
    GROUP BY metric

{% endmacro %}
