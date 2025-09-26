{% macro test_dq0__offer_category__unique_code(model) %}
{% set model = '' %}

    select
        code,
        count(1),
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from {{ ref('offer_category') }}
    group by code
    having count(1) > 1

{% endmacro %}

