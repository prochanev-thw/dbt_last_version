{% macro test_dq0__prvrd_dau_main_metrics__deviation(model) %}
{% set model = '' %}

    with cur_daily_cnt as (
        select 
            ptn_dt, count(1) as cnt, count(distinct coalesce(appmetrica_device_id, contact_id)) as cnt_uniq
        from {{ ref('prvrd_dau_main_metrics') }} 
        where ptn_dt between ('{{ var("dm_date") }}'::date - interval '14 day')::date
            and '{{ var("dm_date") }}'::date
        group by ptn_dt
    ),
    prev_daily_cnt as (
        select
            ptn_dt, count(1) as cnt, count(distinct coalesce(appmetrica_device_id, contact_id)) as cnt_uniq
        from {{ ref('prvrd_dau_main_metrics_last14days') }}
        group by ptn_dt
    )
    select
        ptn_dt,
       cdc.cnt as cur_cnt,
       cdc.cnt_uniq as cur_cnt_uniq,
       pdc.cnt as prev_cnt,
       pdc.cnt_uniq as prev_cnt_uniq,
       round(1.0 * (cdc.cnt - pdc.cnt) / pdc.cnt, 4) as cnt_diff,
       round(1.0 * (cdc.cnt_uniq - pdc.cnt_uniq) / pdc.cnt_uniq, 4) as cnt_uniq_diff,
       '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp as dq_load_datetime
    from cur_daily_cnt as cdc
    join prev_daily_cnt pdc
    using(ptn_dt)

{% endmacro %}
