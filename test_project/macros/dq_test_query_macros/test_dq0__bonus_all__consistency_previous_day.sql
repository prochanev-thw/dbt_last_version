{% macro test_dq0__bonus_all__consistency_previous_day(model) %}
{% set model = '' %}


with
        t1 as (
                select
                    count(*) as t1
                from
                    {{ ref('bonus_all') }}
                where
                    created_on >= to_date('{{ var("dm_date") }}','YYYY-MM-DD') -1
                and
                    created_on < to_date('{{ var("dm_date") }}','YYYY-MM-DD')
                ),


        t2 as (
                select
                    count(*) as t2
                from
                    {{ ref('bonus_all') }}
                where
                    created_on >= to_date('{{ var("dm_date") }}','YYYY-MM-DD') -2
                and
                    created_on < to_date('{{ var("dm_date") }}','YYYY-MM-DD') -1
                 )


select
    t1,
    t2,
    abs(t2 -t1) as difference ,
    '{{ var("dm_date") }}'::date    as dq_date_test,
    current_timestamp               as dq_load_datetime
from
    t1,
    t2
{% endmacro %}
