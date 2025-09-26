{% macro get_delivery_frmt(delivery_type, whs_frmt, partner_name) %}
case
    when {{ delivery_type }} ilike 'dbs' then 'DBS'
    when {{ delivery_type }} ilike any (array['DELIVERY_EXPRESS', 'DELIVERY_SLOT']) and ({{ partner_name }} is not null and {{ partner_name }} != '') then 'DBS'
    when {{ delivery_type }} ilike any (array['DELIVERY_EXPRESS', 'DELIVERY_SLOT']) and ({{ partner_name }} is null or {{ partner_name }} = '') then 'Доставка'
    when {{ delivery_type }} ilike any (array['MAGNIT', 'DELIVERY_SELF', 'PARTNER', 'asap', 'planned']) then 'Доставка'
    when {{ delivery_type }} ilike any (array['TAKEAWAY', 'pickup']) then 'Самовывоз'
    when ({{ delivery_type }} is null or {{ delivery_type }} = '') and {{ whs_frmt }} ilike any (array['МА', 'pharmacy distributor']) then 'Самовывоз'
    else 'Доставка'
end
{% endmacro %}