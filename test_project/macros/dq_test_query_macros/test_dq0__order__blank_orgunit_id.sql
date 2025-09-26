{% macro test_dq0__order__blank_orgunit_id(model) %}
{% set model = '' %}

    select
	    order_pk,
	    order_id,
	    '{{ var("dm_date") }}'::date  as dq_date_test,
	    current_timestamp             as dq_load_datetime
    from {{ ref('order') }}
    where orgunit_id is null
    and dt_load::date = '{{ var("dm_date") }}'::date - interval '2 day'

{% endmacro %}