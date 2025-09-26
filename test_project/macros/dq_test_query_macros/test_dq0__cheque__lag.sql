{% macro test_dq0__cheque__lag(model) %}
{% set model = '' %}


 with
	 slice_dm as (
	 	select
		    dt_load,
		    datetime,
		    sys_change_operation
	 	from
	 		{{ ref('cheque') }}
	 	where
	 		datetime >= '{{ var("dm_date") }}'::date - interval '2 month' -- срез партиций витрины
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
	      when lag_days < 2 then 'lag 1 days'
	      when lag_days = 2 then 'lag 2 days'
	      when lag_days = 3 then 'lag 3 days'
	      when lag_days = 4 then 'lag 4 days'
	      when lag_days = 5 then 'lag 5 days'
	      when lag_days = 6 then 'lag 6 days'
	      when lag_days = 7 then 'lag 7 days'
	      when lag_days >= 8 and lag_days <= 14 then 'lag 8-14 days'
	      when lag_days > 14 and lag_days <= 30 then 'lag 15-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'lag 31-60 days'
	      when lag_days > 60 then 'lag > 60 days'
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
	      when lag_days < 2 then 'lag 1 days'
	      when lag_days = 2 then 'lag 2 days'
	      when lag_days = 3 then 'lag 3 days'
	      when lag_days = 4 then 'lag 4 days'
	      when lag_days = 5 then 'lag 5 days'
	      when lag_days = 6 then 'lag 6 days'
	      when lag_days = 7 then 'lag 7 days'
	      when lag_days >= 8 and lag_days <= 14 then 'lag 8-14 days'
	      when lag_days > 14 and lag_days <= 30 then 'lag 15-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'lag 31-60 days'
	      when lag_days > 60 then 'lag > 60 days'
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
	      when lag_days < 2 then 'lag 1 days'
	      when lag_days = 2 then 'lag 2 days'
	      when lag_days = 3 then 'lag 3 days'
	      when lag_days = 4 then 'lag 4 days'
	      when lag_days = 5 then 'lag 5 days'
	      when lag_days = 6 then 'lag 6 days'
	      when lag_days = 7 then 'lag 7 days'
	      when lag_days >= 8 and lag_days <= 14 then 'lag 8-14 days'
	      when lag_days > 14 and lag_days <= 30 then 'lag 15-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'lag 31-60 days'
	      when lag_days > 60 then 'lag > 60 days'
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


	tm14_dataset as (
	  select
	    dt_load,
	    datetime,
	    sys_change_operation,
	    extract(day from (dt_load - datetime)) as lag_days
	  from
	    slice_dm
	  where dt_load >= '{{ var("dm_date") }}'::date - interval '14 day' 
	  and sys_change_operation = 'I'
	),
	tm14_count_row as (select count(*) as total_count from tm14_dataset
	),
	tm14_lag_groups as (
	  select
	    lag_days,
	    case
	      when lag_days < 2 then 'lag 1 days'
	      when lag_days = 2 then 'lag 2 days'
	      when lag_days = 3 then 'lag 3 days'
	      when lag_days = 4 then 'lag 4 days'
	      when lag_days = 5 then 'lag 5 days'
	      when lag_days = 6 then 'lag 6 days'
	      when lag_days = 7 then 'lag 7 days'
	      when lag_days >= 8 and lag_days <= 14 then 'lag 8-14 days'
	      when lag_days > 14 and lag_days <= 30 then 'lag 15-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'lag 31-60 days'
	      when lag_days > 60 then 'lag > 60 days'
	    end as lag_group
	  from
	    tm14_dataset
	),
	tm14_aggregated_counts as (
	  select
	    l.lag_group,
	    count(*) as cheque_count
	  from
	    tm14_lag_groups l
	  group by
	    l.lag_group
	),
      
    tm30_dataset as (
	  select
	    dt_load,
	    datetime,
	    sys_change_operation,
	    extract(day from (dt_load - datetime)) as lag_days
	  from
	    slice_dm
	  where dt_load >= '{{ var("dm_date") }}'::date - interval '30 day' 
	  and sys_change_operation = 'I'
	),
	tm30_count_row as (select count(*) as total_count from tm30_dataset
	),
	tm30_lag_groups as (
	  select
	    lag_days,
	    case
	      when lag_days < 2 then 'lag 1 days'
	      when lag_days = 2 then 'lag 2 days'
	      when lag_days = 3 then 'lag 3 days'
	      when lag_days = 4 then 'lag 4 days'
	      when lag_days = 5 then 'lag 5 days'
	      when lag_days = 6 then 'lag 6 days'
	      when lag_days = 7 then 'lag 7 days'
	      when lag_days >= 8 and lag_days <= 14 then 'lag 8-14 days'
	      when lag_days > 14 and lag_days <= 30 then 'lag 15-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'lag 31-60 days'
	      when lag_days > 60 then 'lag > 60 days'
	    end as lag_group
	  from
	    tm30_dataset
	),
	tm30_aggregated_counts as (
	  select
	    l.lag_group,
	    count(*) as cheque_count
	  from
	    tm30_lag_groups l
	  group by
	    l.lag_group
	),

    tm60_dataset as (
	  select
	    dt_load,
	    datetime,
	    sys_change_operation,
	    extract(day from (dt_load - datetime)) as lag_days
	  from
	    slice_dm
	  where dt_load >= '{{ var("dm_date") }}'::date - interval '60 day'
	  and sys_change_operation = 'I'
	),
	tm60_count_row as (select count(*) as total_count from tm60_dataset
	),
	tm60_lag_groups as (
	  select
	    lag_days,
	    case
	      when lag_days < 2 then 'lag 1 days'
	      when lag_days = 2 then 'lag 2 days'
	      when lag_days = 3 then 'lag 3 days'
	      when lag_days = 4 then 'lag 4 days'
	      when lag_days = 5 then 'lag 5 days'
	      when lag_days = 6 then 'lag 6 days'
	      when lag_days = 7 then 'lag 7 days'
	      when lag_days >= 8 and lag_days <= 14 then 'lag 8-14 days'
	      when lag_days > 14 and lag_days <= 30 then 'lag 15-30 days'
	      when lag_days > 30 and lag_days <= 60 then 'lag 31-60 days'
	      when lag_days > 60 then 'lag > 60 days'
	    end as lag_group
	  from
	    tm60_dataset
	),
	tm60_aggregated_counts as (
	  select
	    l.lag_group,
	    count(*) as cheque_count
	  from
	    tm60_lag_groups l
	  group by
	    l.lag_group
	)
    

select
  ag.lag_group,
  tm1.cheque_count        as tm1_count,
  tm2.cheque_count        as tm2_count,
  tm7.cheque_count        as tm7_count,
  tm14.cheque_count       as tm14_count,
  tm30.cheque_count       as tm30_count,
  ag.cheque_count         as tm60_count,

  round((tm1.cheque_count::decimal / tm1c.total_count) * 100, 2)     as tm1_percent,
  round((tm2.cheque_count::decimal / tm2c.total_count) * 100, 2)     as tm2_percent,
  round((tm7.cheque_count::decimal / tm7c.total_count) * 100, 2)     as tm7_percent,
  round((tm14.cheque_count::decimal / tm14c.total_count) * 100, 2)   as tm14_percent,
  round((tm30.cheque_count::decimal / tm30c.total_count) * 100, 2)   as tm30_percent,
  round((ag.cheque_count::decimal / ac.total_count) * 100, 2)        as tm60_percent,

  '{{ var("dm_date") }}'::date  as dq_date_test,
  current_timestamp             as dq_load_datetime

from
  tm60_aggregated_counts ag
cross join
  tm60_count_row ac

left join
  tm30_aggregated_counts tm30 on ag.lag_group = tm30.lag_group
cross join
  tm30_count_row tm30c

left join
  tm14_aggregated_counts tm14 on ag.lag_group = tm14.lag_group
cross join
  tm14_count_row tm14c

left join
  tm7_aggregated_counts tm7 on ag.lag_group = tm7.lag_group
cross join
  tm7_count_row tm7c

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