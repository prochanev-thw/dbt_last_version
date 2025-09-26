{% macro test_dq0__ugc_feedback_llm_prediction__raw_cnt(model) %}
{% set model = '' %}
select
    request_dt, 
    count(*) as raw_count,
    '{{ var("dm_date") }}'::date       	 as dq_date_test,
    current_timestamp                    as dq_load_datetime
from
	{{ ref('ugc_feedback_llm_prediction') }}
where 
    request_dt >= '{{ var("dm_date") }}'::date - interval '14 day'
    and request_dt < '{{ var("dm_date") }}'::date
group by 
    request_dt
{% endmacro %}
