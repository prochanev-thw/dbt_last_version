{% macro is_order_completed(order_status_name) %}
 case
    when {{ order_status_name }} ilike any (array['COMPLETED', 'Выполнен', 'Заказ завершен', 'Выдан', 'Доставлен', 'shipped', 'delivery_finished'])
        then True
    else False
 end
{% endmacro %}