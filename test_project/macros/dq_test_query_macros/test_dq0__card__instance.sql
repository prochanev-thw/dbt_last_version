{% macro test_dq0__card__instance(model) %}
{% set model = '' %}

with
    stat__sat_card as (
        select
            scc.instance_id,
            count(1) as last_day_count,
            scc.dt_load::date as dt_load,
            min(scc.modified_on) as min_date,
            max(scc.modified_on) as max_date
        from (
            select
            hc.instance_id,
            sc.dt_load,
            sc.modified_on,
            row_number() over (partition by hc.instance_id, hc.card_id order by sc.version_id desc) as rn
            from {{ ref('sat_card') }} sc
            JOIN {{ ref('hub_card') }} hc ON sc.card_pk = hc.card_pk
            where sc.dt_load >= '{{ var("dm_date") }}'::date - interval '8 day'
            and sc.dt_load < '{{ var("dm_date") }}'::date
            ) scc
        group by scc.instance_id, scc.dt_load::date
        order by scc.dt_load::date desc
    ),

    stat_lag__sat_card as (
        select
            st.instance_id,
            st.last_day_count,
            st.dt_load,
            coalesce(lag(st.dt_load,7) over (partition by instance_id order by dt_load),
            (select min(dt_load) from stat__sat_card where instance_id = stat__sat_card.instance_id)) as dt_lag,
            min_date,
            max_date
        from stat__sat_card as st
    ),

    result_set__sat_card as (
        select
            c1.instance_id,
            round((
                select avg(c2.last_day_count)
                from stat_lag__sat_card as c2
                where c2.instance_id = c1.instance_id and
                c2.dt_load between c1.dt_lag and c1.dt_load - interval '1 day'
            )) as avg_week_count,
            c1.last_day_count,
            round((c1.last_day_count / (
                select avg(c2.last_day_count)
                from stat_lag__sat_card as c2
                where c2.instance_id = c1.instance_id and
                c2.dt_load between c1.dt_lag and c1.dt_load - interval '1 day')
            )* 100, 2) as "percent",
            min_date,
            max_date,
            c1.dt_load,
            row_number() over(partition by instance_id order by dt_load desc)   as rn
        from stat_lag__sat_card as c1
    ),

    final__sat_card as (
	    select
	        t.instance_id                                                     as instance_id,
	        coalesce(rs_data.avg_week_count, rs_old_data.avg_week_count)      as avg_week_count,
	        coalesce(rs_data.last_day_count, 0)                               as last_day_count,
	        case
	            when rs_data.percent is null then 0
	            when rs_data.percent < 3 or rs_data.percent > 1000 then rs_data.percent
	            else rs_data.percent
	        end                                                               as "percent",
	        coalesce(rs_data.min_date, '1900-01-01')                          as min_date,
	        coalesce(rs_data.max_date, '1900-01-01')                          as max_date,
	        coalesce(rs_data.dt_load, '1900-01-01')                           as dt_load,
	        case
	            when rs_data.percent is null then 'No data'
	            when rs_data.percent < 3 or rs_data.percent > 1000 then 'Out of range'
	            else 'In range'
	        end                                                               as dq_result_status,
	        'sat_card'::varchar                                               as dds_sat,

	        '{{ var("dm_date") }}'::date                                      as dq_date_test,
	        current_timestamp                                                 as dq_load_datetime
	    from
	        (values (2), (3), (4), (5), (6), (7), (8), (9), (10),
	              (11), (12), (13), (15), (16), (17), (18), (19),
	              (20), (21), (22), (23), (24), (25), (26), (27),
	              (28), (29), (30), (31), (32), (34), (35)
	        ) as t (instance_id)
	    left join result_set__sat_card as rs_data
	           on t.instance_id = rs_data.instance_id
	          and rs_data.dt_load::date = '{{ var("dm_date") }}'::date - interval '1 day'
	          and rs_data.rn = 1
	    left join result_set__sat_card as rs_old_data
	           on t.instance_id = rs_old_data.instance_id
	          and rs_old_data.dt_load::date < '{{ var("dm_date") }}'::date - interval '1 day'
	          and rs_old_data.rn = 1
	    where
	        rs_data.percent is null or
	        rs_data.percent < 3     or
	        rs_data.percent > 1000
	 ),


    stat__sat_card_loyalty as (
        select
            scc.instance_id,
            count(1) as last_day_count,
            scc.dt_load::date as dt_load,
	        null::timestamp  as min_date,
	        null::timestamp as max_date
        from (
            select
            hc.instance_id,
            sc.dt_load,
            --modified_on,
            row_number() over (partition by hc.instance_id, hc.card_id order by sc.version_id desc) as rn
            from {{ ref('sat_card_loyalty') }} sc
            JOIN {{ ref('hub_card') }} hc ON sc.card_pk = hc.card_pk
                where sc.dt_load >= '{{ var("dm_date") }}'::date - interval '8 day'
                and sc.dt_load < '{{ var("dm_date") }}'::date
            ) scc
        group by scc.instance_id, scc.dt_load::date
        order by scc.dt_load::date desc
    ),

    stat_lag__sat_card_loyalty as (
        select
            st.instance_id,
            st.last_day_count,
            st.dt_load,
            coalesce(lag(st.dt_load,7) over (partition by instance_id order by dt_load),
            (select min(dt_load) from stat__sat_card_loyalty where instance_id = stat__sat_card_loyalty.instance_id)) as dt_lag,
            min_date,
            max_date
        from stat__sat_card_loyalty as st
    ),

    result_set__sat_card_loyalty as (
        select
            c1.instance_id,
            round((
                select avg(c2.last_day_count)
                from stat_lag__sat_card_loyalty as c2
                where c2.instance_id = c1.instance_id and
                c2.dt_load between c1.dt_lag and c1.dt_load - interval '1 day'
            )) as avg_week_count,
            c1.last_day_count,
            round((c1.last_day_count / (
                select avg(c2.last_day_count)
                from stat_lag__sat_card_loyalty as c2
                where c2.instance_id = c1.instance_id and
                c2.dt_load between c1.dt_lag and c1.dt_load - interval '1 day')
            )* 100, 2) as "percent",
            min_date,
            max_date,
            c1.dt_load,
            row_number() over(partition by instance_id order by dt_load desc)   as rn
        from stat_lag__sat_card_loyalty as c1
	),

	final__sat_card_loyalty as (
		select
		    t.instance_id                                                     as instance_id,
		    coalesce(rs_data.avg_week_count, rs_old_data.avg_week_count)      as avg_week_count,
		    coalesce(rs_data.last_day_count, 0)                               as last_day_count,
		    case
		        when rs_data.percent is null then 0
		        when rs_data.percent < 3 or rs_data.percent > 1000 then rs_data.percent
		        else rs_data.percent
		    end                                                               as "percent",
		    coalesce(rs_data.min_date, '1900-01-01')                          as min_date,
		    coalesce(rs_data.max_date, '1900-01-01')                          as max_date,
		    coalesce(rs_data.dt_load, '1900-01-01')                           as dt_load,
		    case
		        when rs_data.percent is null then 'No data'
		        when rs_data.percent < 3 or rs_data.percent > 1000 then 'Out of range'
		        else 'In range'
		    end                                                               as dq_result_status,
		    'sat_card_loyalty'::varchar                                       as dds_sat,

		    '{{ var("dm_date") }}'::date                                      as dq_date_test,
		    current_timestamp                                                 as dq_load_datetime
		from
		    (values (2), (3), (4), (5), (6), (7), (8), (9), (10),
		          (11), (12), (13), (15), (16), (17), (18), (19),
		          (20), (21), (22), (23), (24), (25), (26), (27),
		          (28), (29), (30), (31), (32), (34)
		    ) as t (instance_id)
		left join result_set__sat_card_loyalty as rs_data
		       on t.instance_id = rs_data.instance_id
		      and rs_data.dt_load::date = '{{ var("dm_date") }}'::date - interval '1 day'
		      and rs_data.rn = 1
		left join result_set__sat_card_loyalty as rs_old_data
		       on t.instance_id = rs_old_data.instance_id
		      and rs_old_data.dt_load::date < '{{ var("dm_date") }}'::date - interval '1 day'
		      and rs_old_data.rn = 1
		where
		    rs_data.percent is null
	)

select * from final__sat_card
union all
select * from final__sat_card_loyalty

{% endmacro %}