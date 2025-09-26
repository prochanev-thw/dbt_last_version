{% macro test_dq0__bonus_all_optimized__instance_bonus_wo(model) %}
{% set model = '' %}

    with
        stat as (
            select
                scc.instance_id,
                count(1) as last_day_count,
                scc.dt_load::date as dt_load,
                min(scc.created_on) as min_date,
                max(scc.created_on) as max_date
            from (
                select
                s.instance_id,
                s.dt_load,
                s.created_on
                from {{ ref('bonus_write_off') }} s
                where s.dt_load >= '{{ var("dm_date") }}'::date - interval '8 day'
                and s.dt_load < '{{ var("dm_date") }}'::date) scc
            group by scc.instance_id, scc.dt_load::date
        ),

        min_dates_inst as (
            select
                instance_id,
                min(dt_load) as min_dt_load
            from stat
            group by instance_id
        ),

        stat_lag as (
            select
                stat.instance_id,
                stat.last_day_count,
                stat.dt_load,
                coalesce(lag(stat.dt_load,7) over (partition by stat.instance_id order by stat.dt_load),
                    min_dates_inst.min_dt_load
                ) as dt_lag,
                min_date,
                max_date
            from stat
            join min_dates_inst ON stat.instance_id = min_dates_inst.instance_id
        ),

        result_set as (
            select
                c1.instance_id,
                round((
                    select avg(c2.last_day_count)
                    from stat_lag as c2
                    where c2.instance_id = c1.instance_id and
                    c2.dt_load between c1.dt_lag and c1.dt_load - interval '1 day'
                )) as avg_week_count,
                c1.last_day_count,
                round((c1.last_day_count / (
                    select avg(c2.last_day_count)
                    from stat_lag as c2
                    where c2.instance_id = c1.instance_id and
                    c2.dt_load between c1.dt_lag and c1.dt_load - interval '1 day')
                )* 100, 2) as "percent",
                min_date,
                max_date,
                c1.dt_load,
                row_number() over(partition by instance_id order by dt_load desc)   as rn
            from stat_lag as c1
    )

    select
        t.instance_id                                                     as instance_id,
        coalesce(rs_data.avg_week_count, rs_old_data.avg_week_count)      as avg_week_count,
        coalesce(rs_data.last_day_count, 0)                               as last_day_count,
        case
            when rs_data.percent is null then 0
            when rs_data.percent < 5 or rs_data.percent > 1000 then rs_data.percent
            else rs_data.percent
        end                                                               as "percent",
        coalesce(rs_data.min_date, '1900-01-01')                          as min_date,
        coalesce(rs_data.max_date, '1900-01-01')                          as max_date,
        coalesce(rs_data.dt_load, '1900-01-01')                           as dt_load,
        case
            when rs_data.percent is null then 'No data'
            when rs_data.percent < 5 or rs_data.percent > 1000 then 'Out of range'
            else 'In range'
        end                                                               as dq_result_status,
        '{{ var("dm_date") }}'::date                                      as dq_date_test,
        current_timestamp                                                 as dq_load_datetime
    from
        (values (2), (3), (4), (5), (6), (7), (8), (9), (10),
              (11), (12), (13), (15), (16), (17), (18), (19),
              (20), (21), (22), (23), (24), (25), (26), (27),
              (28), (29), (30), (31), (32), (34), (35)
        ) as t (instance_id)
    left join result_set as rs_data
           on t.instance_id = rs_data.instance_id
          and rs_data.dt_load::date = '{{ var("dm_date") }}'::date - interval '1 day'
          and rs_data.rn = 1
    left join result_set as rs_old_data
           on t.instance_id = rs_old_data.instance_id
          and rs_old_data.dt_load::date < '{{ var("dm_date") }}'::date - interval '1 day'
          and rs_old_data.rn = 1
    where
        rs_data.percent is null or
        rs_data.percent < 3     or
        rs_data.percent > 1000

{% endmacro %}