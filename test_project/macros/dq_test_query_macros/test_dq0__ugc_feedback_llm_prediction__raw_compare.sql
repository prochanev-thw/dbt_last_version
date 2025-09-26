{% macro test_dq0__ugc_feedback_llm_prediction__raw_compare(model) %}
{% set model = '' %}
WITH dm_cntd AS (
        select
        count(distinct feedback_id) as cntd
    from {{ ref('ugc_feedback_llm_prediction') }}
    where request_dt = '{{ var("dm_date") }}'::date - interval '1 day'
),
union_all_pl as (
    select 
        ugc_feedback_llm_prediction_pk
    from {{ ref('sat_ugc_feedback_llm_prediction_ba_ma') }}
    where request_dttm::date = '{{ var("dm_date") }}'::date - interval '1' day
    union all
    select 
        ugc_feedback_llm_prediction_pk
    from {{ ref('sat_ugc_feedback_llm_prediction_ba_mc') }}
    where request_dttm::date = '{{ var("dm_date") }}'::date - interval '1' day
    union all
    select 
        ugc_feedback_llm_prediction_pk
    from {{ ref('sat_ugc_feedback_llm_prediction_off_food_bas') }}
    where request_dttm::date = '{{ var("dm_date") }}'::date - interval '1' day
    union all
    select 
        ugc_feedback_llm_prediction_pk
    from {{ ref('sat_ugc_feedback_llm_prediction_off_incident') }}
    where request_dttm::date = '{{ var("dm_date") }}'::date - interval '1' day
    union all
    select 
        ugc_feedback_llm_prediction_pk
    from {{ ref('sat_ugc_feedback_llm_prediction_on_incident') }}
    where request_dttm::date = '{{ var("dm_date") }}'::date - interval '1' day
    union all
    select 
        ugc_feedback_llm_prediction_pk
    from {{ ref('sat_ugc_feedback_llm_prediction_online_ba') }}
    where request_dttm::date = '{{ var("dm_date") }}'::date - interval '1' day
),
dds_cntd AS (
    select
        count(distinct feedback_id) as cntd
    from union_all_pl as sat
    join  {{ ref('hub_ugc_feedback_llm_prediction') }} as hub
    using (ugc_feedback_llm_prediction_pk)
)
select
    (select * from dm_cntd) as dm_cntd,
    (select * from dds_cntd) as dds_cntd,
    '{{ var("dm_date") }}'::date as dq_date_test,
    current_timestamp as dq_load_datetime
{% endmacro %}
