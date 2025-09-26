{% macro test_dq0__reviews_feedbacks_bacth__unique_id(model) %}
{% set model = '' %}

with dublicate_id as (select id
                     from {{ ref('reviews_feedbacks_batch') }}
                     group by 1
                     having count(1) > 1)
select id
     , '{{ var("dm_date") }}'::date as dq_date_test
     , now()        as dq_load_datetime
from dublicate_id

{% endmacro %}

