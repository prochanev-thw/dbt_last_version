{% macro test_dq0__card__unique_card_number(model) %}
{% set model = '' %}

with
    t_card_duple as (
        select
            c.card_number,
            count(1) as "count"
        from dm.card c
        where c.record_source = 'manzana'
        group by c.card_number
        having count(1) > 1
    ),

    card_attr as (
        select
            d.card_number,
            d."count",
            case when ce.name = 'is_moving' and ce.value_num = 1000 then 1 else 0 end as is_moving
        from t_card_duple d
        left join dm.card c on
            c.card_number = d.card_number
        left join dm.card_ea ce on
            ce.card_pk = c.card_pk
        group by d.card_number, d."count", is_moving
    )

select
	ca.card_number                  as card_number,
	ca."count"                      as "count",
    '{{ var("dm_date") }}'::date    as dq_date_test,
    current_timestamp               as dq_load_datetime,
	max(ca.is_moving)               as is_moving
from card_attr ca
group by card_number, "count"

{% endmacro %}
