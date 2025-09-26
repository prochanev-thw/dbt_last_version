{% macro test_dq0__agg_dac_daily_predictions__check_dac_fact(model) %}
{% set model = '' %}

    with tt1 as (
        select
            dac_fact AS check_1,
            '{{ var("dm_date") }}'::date  as dq_date_test,
            current_timestamp             as dq_load_datetime
        from {{ ref('agg_dac_daily_predictions') }}
        where date_load = '{{ var("dm_date") }}'::date - interval '1'day
    ),
    tt2 as (
        select  dac_fact AS check_0
        from {{ ref('agg_dac_daily_predictions') }}
        WHERE date_load = '{{ var("dm_date") }}'::date - interval '2'day

    )
    select
    	check_1,
        case
	        when check_1 = 0
	        then 1
	        else 0
        end                                             as check_1_result,
		check_0,
		(check_1::NUMERIC / check_0::NUMERIC) - 1       as "calc_result",
        case
        	when ((check_1 / check_0) - 1) < 0.001
        	then 1
        	else 0
        end                                             as check_0_result ,
        '{{ var("dm_date") }}'::date                    as dq_date_test,
        current_timestamp                               as dq_load_datetime
    from  tt1
    left join tt2 on 1=1



{% endmacro %}

