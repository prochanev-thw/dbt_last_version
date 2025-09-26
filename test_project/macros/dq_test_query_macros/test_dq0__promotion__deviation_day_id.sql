{% macro test_dq0__promotion__deviation_day_id (model) %}
{% set model = '' %}

with
	calc_data as (
		select
            day_id,
            cnt_row,
            round(sum(cnt_row) over() / count(day_id) over()) as avg_cnt_row,
            round(((cnt_row / (sum(cnt_row) over() / count(day_id) over()))-1)*100) as deviation_day_id_row
		from (
			select
				day_id,
				count(day_id) as cnt_row
			from {{ ref('promotion') }}
		    where day_id between '{{ var("dm_date") }}'::date - interval '15 day' and
		                         '{{ var("dm_date") }}'::date - interval '3 day'
			group by day_id

		)t
		group by day_id, cnt_row
	)

select
	day_id                                 as day_id,
	cnt_row                                as cnt_row_on_day_id,
	avg_cnt_row                            as avg_cnt_row_on_period_check,
	coalesce(cd.deviation_day_id_row, 0)   as deviation_day_id_row_for_avg_day_on_period_check_in_percent,
    '{{ var("dm_date") }}'::date           as dq_date_test,
    current_timestamp                      as dq_load_datetime
from calc_data cd
where deviation_day_id_row not between -49 AND 49
order by day_id desc

{% endmacro %}
