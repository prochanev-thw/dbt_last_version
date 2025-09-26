{% macro test_dq0__contact__unique_contact_id(model) %}
{% set model = '' %}

    select
        contact_id,
        count(1),
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from {{ ref('contact') }}
    group by contact_id
    having count(1) > 1

{% endmacro %}

