{% macro test_dq0__cheque__unique_number(model) %}
{% set model = '' %}

    with
         main_dataset as (
            select
                "number"                      as "number",
                count(1)                      as count,
                sum(summ_discounted)          as summ,
                '{{ var("dm_date") }}'::date  as dq_date_test,
                current_timestamp             as dq_load_datetime
            from {{ ref('cheque') }}
            where record_source = 'manzana'
              and operation_type_id = 1
            group by "number", operation_type_id
            having count(1) > 1
        )
    select
        "number",
        count,
        summ/count as summ,
        dq_date_test,
        dq_load_datetime
    from main_dataset

{% endmacro %}


