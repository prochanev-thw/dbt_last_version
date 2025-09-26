{% macro test_dq0__raw_prvdr_feature_core__check_sum(model) %}
{% set model = '' %}

    select 
        sum(1) as raws_cnt,
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp as dq_load_datetime
    from {{ ref('raw_prvdr_feature_core') }} 
    where event_date > '{{ var("dm_date") }}'::date - interval '1 day'
        or event_date < '{{ var("dm_date") }}'::date - interval '3 day'

{% endmacro %}
