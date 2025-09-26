{% macro is_order_canceled(order_status_name) %}
 case
    when {{ order_status_name }} ilike any (array['CANCELED', 'canceled', 'Отменен', 'Не выкуплен', 'Невыкуп', 'ORDER_NOT_RECEIVED_NOT_PAID', 'ORDER_NOT_RECEIVED', 'Не доставлен', 'cancelled_by_service', 'cancelled_with_payment', 'cancelled'])
        then True
    else False
 end
{% endmacro %}