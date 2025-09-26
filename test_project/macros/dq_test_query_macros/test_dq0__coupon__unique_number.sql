{% macro test_dq0__coupon__unique_number(model) %}
{% set model = '' %}

    select
       number,
       count(1),
       '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp             as dq_load_datetime
    from {{ ref('coupon') }}
    group by number
    having count(1) > 1

{% endmacro %}
