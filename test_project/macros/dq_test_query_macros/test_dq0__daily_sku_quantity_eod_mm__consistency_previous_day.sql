{% macro test_dq0__daily_sku_quantity_eod_mm__consistency_previous_day(model) %}
{% set model = '' %}


with t1 as (
	select 
		count(1) as t1
	from {{ ref('daily_sku_quantity_eod_mm') }}
	where dt >= to_date('{{ var("dm_date") }}', 'YYYY-MM-DD') - 2
		and dt < to_date('{{ var("dm_date") }}', 'YYYY-MM-DD') - 1
),
t2 as (
	select 
		count(1) as t2
	from {{ ref('daily_sku_quantity_eod_mm') }}
	where dt >= to_date('{{ var("dm_date") }}', 'YYYY-MM-DD') - 1
		and dt < to_date('{{ var("dm_date") }}', 'YYYY-MM-DD')
)
select 
    t1,
    t2,
	round((1 - (t1)::numeric(10,2)/(t2)::numeric(10,2)) * 100, 2) as difference,
	'{{ var("dm_date") }}'::date as dq_date_test,
	current_timestamp as dq_load_datetime
from
	t1,
	t2
{% endmacro %}
