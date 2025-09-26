{% macro test_dq0__cheque_item__consistency_article_id(model) %}
{% set model = '' %}

    with cheque_item as
    (
    select cheque_id,
           cheque_pk,
           cheque_item_id,
           cheque_item_pk,
           article_id,
           dt_load
    from {{ ref('cheque_item') }}
    where dt_load >= '{{ var("dm_date") }}'::date - interval '2 day'and dt_load < '{{ var("dm_date") }}'::date
    )
    select ci.cheque_id,
           ci.cheque_pk,
           ci.cheque_item_id,
           ci.cheque_item_pk,
           ci.article_id,
           ci.dt_load,
           '{{ var("dm_date") }}'::date  as dq_date_test,
           current_timestamp             as dq_load_datetime
    from cheque_item ci
    left join {{ ref('hub_article') }} ac
    on ci.article_id = ac.article_id
    where ac.article_id is null

{% endmacro %}