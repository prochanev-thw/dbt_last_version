{% macro test_dq0__cheque_item__lag(model) %}
{% set model = '' %}


 with
	 slice_dm as (
	 	select
		    dt_load,
		    datetime,
		    sys_change_operation
	 	from
	 		{{ ref('cheque_item') }}
	 	where
	 		datetime >= '{{ var("dm_date") }}'::date - interval '2 month' -- срез партиций витрины
	 ),

	tm7_dataset as (
	  select
	    dt_load,
	    datetime,
	    sys_change_operation,
	    extract(day from (dt_load - datetime)) as lag_days
	  from
	    slice_dm
	  where dt_load >= '{{ var("dm_date") }}'::date - interval '7 day' -- >=T-7
	  and sys_change_operation = 'I'
	),
	tm7_count_row as (select count(*) as total_count from tm7_dataset
	),
	tm7_lag_groups as (
	  select
	    lag_days,
	    case
	      when lag_days < 2 then 'group_1: lag 1 days'
	      when lag_days = 2 then 'group_2: lag 2 days'
	      when lag_days = 3 then 'group_3: lag 3 days'
	      when lag_days = 4 then 'group_4: lag 4 days'
	      when lag_days = 5 then 'group_5: lag 5 days'
	      when lag_days = 6 then 'group_6: lag 6 days'
	      when lag_days = 7 then 'group_7: lag 7 days'
	      when lag_days >= 8 and lag_days <= 30 then 'group_8: lag 8-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'group_9: lag 31-60 days'
	      when lag_days > 60 then 'group_x: lag > 60 days'
	    end as lag_group
	  from
	    tm7_dataset
	),
	tm7_aggregated_counts as (
	  select
	    l.lag_group,
	    count(*) as cheque_count
	  from
	    tm7_lag_groups l
	  group by
	    l.lag_group
	),


	tm1_dataset as (
	  select
	    dt_load,
	    datetime,
	    sys_change_operation,
	    extract(day from (dt_load - datetime)) as lag_days
	  from
	    slice_dm
	  where dt_load >= '{{ var("dm_date") }}'::date - interval '1 day' -- =T-1
	  and sys_change_operation = 'I'
	),
	tm1_count_row as (select count(*) as total_count from tm1_dataset
	),
	tm1_lag_groups as (
	  select
	    lag_days,
	    case
	      when lag_days < 2 then 'group_1: lag 1 days'
	      when lag_days = 2 then 'group_2: lag 2 days'
	      when lag_days = 3 then 'group_3: lag 3 days'
	      when lag_days = 4 then 'group_4: lag 4 days'
	      when lag_days = 5 then 'group_5: lag 5 days'
	      when lag_days = 6 then 'group_6: lag 6 days'
	      when lag_days = 7 then 'group_7: lag 7 days'
	      when lag_days >= 8 and lag_days <= 30 then 'group_8: lag 8-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'group_9: lag 31-60 days'
	      when lag_days > 60 then 'group_x: lag > 60 days'
	    end as lag_group
	  from
	    tm1_dataset
	),
	tm1_aggregated_counts as (
	  select
	    l.lag_group,
	    count(*) as cheque_count
	  from
	    tm1_lag_groups l
	  group by
	    l.lag_group
	),


	tm2_dataset as (
	  select
	    dt_load,
	    datetime,
	    sys_change_operation,
	    extract(day from (dt_load - datetime)) as lag_days
	  from
	    slice_dm
	  where dt_load >= '{{ var("dm_date") }}'::date - interval '2 day' -- >=T-2
	  and sys_change_operation = 'I'
	),
	tm2_count_row as (select count(*) as total_count from tm2_dataset
	),
	tm2_lag_groups as (
	  select
	    lag_days,
	    case
	      when lag_days < 2 then 'group_1: lag 1 days'
	      when lag_days = 2 then 'group_2: lag 2 days'
	      when lag_days = 3 then 'group_3: lag 3 days'
	      when lag_days = 4 then 'group_4: lag 4 days'
	      when lag_days = 5 then 'group_5: lag 5 days'
	      when lag_days = 6 then 'group_6: lag 6 days'
	      when lag_days = 7 then 'group_7: lag 7 days'
	      when lag_days >= 8 and lag_days <= 30 then 'group_8: lag 8-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'group_9: lag 31-60 days'
	      when lag_days > 60 then 'group_x: lag > 60 days'
	    end as lag_group
	  from
	    tm2_dataset
	),
	tm2_aggregated_counts as (
	  select
	    l.lag_group,
	    count(*) as cheque_count
	  from
	    tm2_lag_groups l
	  group by
	    l.lag_group
	)

select
  ag.lag_group,
  tm1.cheque_count as tm1_count,
  tm2.cheque_count as tm2_count,
  ag.cheque_count as tm7_count,
  round((tm1.cheque_count::decimal / tm1c.total_count) * 100, 2) as tm1_percent,
  round((tm2.cheque_count::decimal / tm2c.total_count) * 100, 2) as tm2_percent,
  round((ag.cheque_count::decimal / ac.total_count) * 100, 2) as tm7_percent,
  '{{ var("dm_date") }}'::date  as dq_date_test,
  current_timestamp             as dq_load_datetime
from
  tm7_aggregated_counts ag
cross join
  tm7_count_row ac
left join
  tm1_aggregated_counts tm1 on ag.lag_group = tm1.lag_group
cross join
  tm1_count_row tm1c
left join
  tm2_aggregated_counts tm2 on ag.lag_group = tm2.lag_group
cross join
  tm2_count_row tm2c
order by
  ag.lag_group

{% endmacro %}