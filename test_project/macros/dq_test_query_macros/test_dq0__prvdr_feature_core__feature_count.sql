{% macro test_dq0__prvdr_feature_core__feature_count(model) %}
{% set model = '' %}

    select
       feature_name, 
       count(1) as feature_count,
       '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp             as dq_load_datetime
    from {{ ref('prvdr_feature_core') }}
    where event_date = '{{ var("dm_date") }}'::date - interval '1 day'
    group by feature_name

{% endmacro %}
