{% macro test_dq0__offer_perspromo__consistency_check_last_day(model) %}
{% set model = '' %}


with
        table_1 as (
                select
                    count(*) as diff_one_day
                from
                    {{ ref('offer_perspromo') }}
                where
                    load_datetime >= to_date('{{ var("dm_date") }}','YYYY-MM-DD') -1
                and
                    load_datetime < to_date('{{ var("dm_date") }}','YYYY-MM-DD')
                )


select
		diff_one_day,
		'{{ var("dm_date") }}'::date            as dq_date_test,
		current_timestamp                       as dq_load_datetime
from
		table_1
{% endmacro %}
