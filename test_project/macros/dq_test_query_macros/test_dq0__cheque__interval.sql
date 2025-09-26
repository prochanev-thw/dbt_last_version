{% macro test_dq0__cheque__interval(model) %}
{% set model = '' %}

select
    cheque_pk,
    '{{ var("dm_date") }}'::date  as dq_date_test,
    current_timestamp             as dq_load_datetime
from {{ ref('cheque') }}
where datetime not between '2022-07-25' and now()
    AND record_source = 'manzana'

{% endmacro %}


