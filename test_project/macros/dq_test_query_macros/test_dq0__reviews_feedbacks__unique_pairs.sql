{% macro test_dq0__reviews_feedbacks__unique_pairs(model) %}
{% set model = '' %}

with uniq_config as (select distinct config_id, object_type_id
                     from {{ ref('reviews_feedbacks') }}
                     where dt < '{{ var("dm_date") }}'::date)
select config_id
     , object_type_id
     , '{{ var("dm_date") }}'::date as dq_date_test
     , now()        as dq_load_datetime
from uniq_config





{% endmacro %}

