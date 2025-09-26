{% macro test_dq0__cheque_card_whs_new_ch_batch__clickhouse_consist(model) %}
{% set model = '' %}

with clickhousecounts as (
    select
        cnt,
        row_number() over (order by dq_load_datetime desc) as rn
    from dwh.sandbox.dq0__cheque_card_whs_new_ch_batch__clickhouse_consist_external
    where dq_date_test = '{{ var("dm_date") }}'::date
)

select
    count(cheque_pk)                                 as cnt_cheque_greenplum,
    (select cnt from clickhousecounts where rn = 1)  as cnt_cheque_clickhouse,
    '{{ var("dm_date") }}'::date                     as dq_date_test,
    current_timestamp                                as dq_load_datetime
from {{ ref('cheque') }}
where datetime >='2023-01-01'
and datetime < date_trunc('month', current_timestamp) + interval '1 month'

{% endmacro %}
