{% macro test_dq0__contact_daily_transactions_pl_registered__check_big_cheque(model) %}
{% set model = '' %}

    select 
        count(*) as cheque_cnt,
	    array_agg(cheque_pk::text) as cheque_pk_arr,
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp as dq_load_datetime
    from {{ ref('cheque') }} 
    where "datetime" >= '{{ var("dm_date") }}'::date - interval '1 day'
        and summ >= 1000000
    limit 1
{% endmacro %}
