{% macro test_dq0__agg_dac_monthly_facts__check_dac_fact(model) %}
{% set model = '' %}

    with tt1 as (
        select
            dac_fact,
            '{{ var("dm_date") }}'::date  as dq_date_test,
            current_timestamp             as dq_load_datetime
        from {{ ref('agg_dac_monthly_facts') }}
        where date(load_datetime)  = '{{ var("dm_date") }}'::date
        and date_month = date_trunc('month', '{{ var("dm_date") }}'::date)
    )
    select
    	dac_fact,
        case
	        when dac_fact = 0
	        then 1
	        else 0
        end                                             as dac_fact_result,
        '{{ var("dm_date") }}'::date                    as dq_date_test,
        current_timestamp                               as dq_load_datetime
    from  tt1



{% endmacro %}

