{% macro test_dq0__contact_daily_transactions_pl_registered__unique_contact(model) %}
{% set model = '' %}

    --витрина огромная поэтому чекаем дубли только за 2 месяца, инкремент там 14 дней поэтому появиться им просто будет не откуда
    select
       contact_id, 
       dt, 
       count(1) as count,
       '{{ var("dm_date") }}'::date  as dq_date_test,
       current_timestamp             as dq_load_datetime
    from {{ ref('contact_daily_transactions_pl_registered') }}
    where dt >= date_trunc('month', '{{ var("dm_date") }}'::date)::date - interval '1 month'
    group by contact_id, dt
    having count(1) > 1

{% endmacro %}
