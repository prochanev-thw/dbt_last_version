{% macro test_dq0__ft_order_delivery_address_oms_private__unique_ecom_order(model) %}
{% set model = '' %}

    select
        ecom_order_pk,
        count(1),
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from {{ ref('ft_order_delivery_address_oms_private') }}
    group by ecom_order_pk
    having count(1) > 1

{% endmacro %}