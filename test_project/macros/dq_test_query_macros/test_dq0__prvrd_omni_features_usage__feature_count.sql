{% macro test_dq0__prvrd_omni_features_usage__feature_count(model) %}
{% set model = '' %}

    select
       feature, 
       count(1) as feature_count,
       '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp             as dq_load_datetime
    from {{ ref('prvrd_omni_features_usage') }}
    where event_date = '{{ var("dm_date") }}'::date - interval '1 day'
    group by feature

{% endmacro %}
