{% macro test_dq0__tg_metric_daily__check_max_day(model) %}
{% set model = '' %}

    SELECT
        frmt_eng,
        metric,
        max(dt) AS max_date,
        CASE
            WHEN max(dt) = current_date - 1 THEN 1
            ELSE 0
        END AS date_check_result,
        max(dt) - (current_date - INTERVAL '1 days') count_miss_actual_day,
        '{{ var("dm_date") }}'::date as dq_date_test,
        current_timestamp as dq_load_datetime
    FROM {{ ref('tg_metric_daily_format') }}
    WHERE frmt_eng != 'Мpharm'
    GROUP BY frmt_eng, metric
    UNION ALL
    SELECT
        'Total'::varchar frmt,
        metric,
        max(dt) AS max_date,
        CASE
            WHEN max(dt) = current_date - 1
            THEN 1
            ELSE 0
        END AS date_check_result,
        max(dt) - (current_date - INTERVAL '1 days') count_miss_actual_day,
        '{{ var("dm_date") }}'::date as dq_date_test,
        current_timestamp as dq_load_datetime
    FROM {{ ref('tg_metric_daily') }}
    GROUP BY metric

{% endmacro %}
