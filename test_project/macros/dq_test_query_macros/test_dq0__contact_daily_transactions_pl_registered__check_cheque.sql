{% macro test_dq0__contact_daily_transactions_pl_registered__check_cheque(model) %}
{% set model = '' %}
    with check_count as (
        select
            count(*) as cheque_cnt,
            "datetime"::date as cheque_date
        from {{ ref('cheque') }} 
        where "datetime"::date>= '{{ var("dm_date") }}'::date - interval '365 day'
            and "datetime"::date <= '{{ var("dm_date") }}'::date - interval '1 day'
            and summ > 100000
        group by 
            "datetime"::date
    ), mid_cheque_365_days as (
        select 
            avg(cheque_cnt) as avg_cheque_cnt,
            STDDEV(cheque_cnt) as std_cheque_cnt
        from check_count
    ),
    big_cheque as (
        select 
            cheque_pk
        from {{ ref('cheque') }}  as c 
        where "datetime"::date = '{{ var("dm_date") }}'::date - interval '1 day'
            and summ > 100000
    )
    select 
       	count(*) as cheque_cnt,
	    array_agg(cheque_pk::text) as cheque_pk_arr,
        '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp as dq_load_datetime
    from big_cheque as c 
    where (select count(*) from big_cheque) > (select avg_cheque_cnt + 1 * std_cheque_cnt from mid_cheque_365_days)
    limit 1
{% endmacro %}
