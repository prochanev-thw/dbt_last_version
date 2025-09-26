{% macro test_dq0__reviews_stores_cx__unique_config_id(model) %}
{% set model = '' %}

with uniq_config as (select distinct config_id
                     from {{ ref('reviews_stores_cx') }}
                     where dt < '{{ var("dm_date") }}'::date)
select config_id
     , '{{ var("dm_date") }}'::date as dq_date_test
     , now()        as dq_load_datetime
from uniq_config

{% endmacro %}