{% macro test_dq0__cheque_item__consistency_article_id_null(model) %}
{% set model = '' %}

    select cheque_item_pk,
           cheque_item_id,
           cheque_pk,
           cheque_id,
           '{{ var("dm_date") }}'::date  as dq_date_test,
           current_timestamp            as dq_load_datetime
    from {{ ref('cheque_item') }} ci
    where record_source = 'manzana'
    and article_id is null
    and dt_load::date = '{{ var("dm_date") }}'::date - interval '2 day'

{% endmacro %}