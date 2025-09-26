{% macro test_dq0__contact_daily_transactions_pl_registered__check_sum(model) %}
{% set model = '' %}

    with check_sum as (
        select 
            sum(1) as sum
        from {{ ref('contact_daily_transactions_pl_registered') }} 
        where dt = '{{ var("dm_date") }}'::date - interval '1 day'
    ),
    check_sum_2 as (
        select 
            sum(1) as sum
        from {{ ref('contact_daily_transactions_pl_registered') }} 
        where dt >= '{{ var("dm_date") }}'::date - interval '8 day'
            and dt < '{{ var("dm_date") }}'::date - interval '1 day'
    )
    select
       (case
            when cs.sum < (cs_2.sum::numeric / 7::numeric) * 0.6
                then 0::smallint --test failed
            else
                1::smallint -- test passed
       end) as result,
       cs.sum as sum,
       cs_2.sum as sum_2,
       '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp as dq_load_datetime
    from check_sum as cs
    cross join check_sum_2 as cs_2

{% endmacro %}
