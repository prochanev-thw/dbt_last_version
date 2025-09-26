{% macro test_dq0__offer__unique_code(model) %}
{% set model = '' %}

    select
        code,
        count(1),
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from {{ ref('offer') }}
    where record_source = 'manzana'
      and is_active = 1
    group by code
    having count(1) > 1

{% endmacro %}
