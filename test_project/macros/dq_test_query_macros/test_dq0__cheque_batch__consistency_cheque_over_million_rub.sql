{% macro test_dq0__cheque_batch__consistency_cheque_over_million_rub(model) %}
{% set model = '' %}

select
    last_version,
    cheque_pk,
    datetime,
    summ_discounted,
    dt_load,
    '{{ var("dm_date") }}'::date        as dq_date_test,
    current_timestamp                   as dq_load_datetime
from {{ ref('cheque_batch') }}
where record_source = 'manzana'
and sys_change_operation != 'D'
and summ_discounted >= '1000000'
{% endmacro %}
