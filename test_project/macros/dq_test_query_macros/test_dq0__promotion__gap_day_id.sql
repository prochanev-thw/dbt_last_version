{% macro test_dq0__promotion__gap_day_id(model) %}
{% set model = '' %}

with
	date_id_series as (
  		select generate_series('{{ var("dm_date") }}'::date - interval '15 day',
  		                       '{{ var("dm_date") }}'::date - interval '3 day', '1 day') as day_id
	),

	date_id_real as (
		select
			day_id
		from {{ ref('promotion') }}
		where day_id between '{{ var("dm_date") }}'::date - interval '15 day' and
		                     '{{ var("dm_date") }}'::date - interval '3 day'
		group by day_id
	)

select
	ds.day_id::date               as day_id,
    '{{ var("dm_date") }}'::date  as dq_date_test,
    current_timestamp             as dq_load_datetime
from date_id_series ds
left join  date_id_real cd on ds.day_id = cd.day_id
where cd.day_id is null
order by ds.day_id desc

{% endmacro %}
