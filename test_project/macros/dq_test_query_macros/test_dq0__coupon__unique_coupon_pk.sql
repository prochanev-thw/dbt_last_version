{% macro test_dq0__coupon__unique_coupon_pk(model) %}
{% set model = '' %}

    select
       coupon_pk,
       count(1),
       '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp             as dq_load_datetime
    from {{ ref('coupon') }}
    group by coupon_pk
    having count(1) > 1

{% endmacro %}
