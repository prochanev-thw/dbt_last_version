{% macro test_dq0__ft_order_delivery_address_oms_private__unique_order_id(model) %}
{% set model = '' %}

    select
        order_id,
        count(1),
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from {{ ref('ft_order_delivery_address_oms_private') }}
    group by order_id
    having count(1) > 1

{% endmacro %}