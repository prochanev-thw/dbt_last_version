{% macro test_dq0__club__unique_club_id(model) %}
{% set model = '' %}

    select
        club_id,
        count(1),
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from {{ ref('club') }}
    group by club_id
    having count(1) > 1

{% endmacro %}

