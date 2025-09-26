{% macro test_dq0__cheque__blank_card_number(model) %}
{% set model = '' %}

    with
        sat_card_del as (
            select
                sc.card_pk,
                card_number,
                deleted_flg,
                dt_load,
                card_id,
                instance_id,
                row_number() over(partition by sc.card_pk order by version_id desc) rn
            from
                {{ ref('sat_card') }} sc
                left join {{ ref('hub_card') }} hc on hc.card_pk = sc.card_pk
        ),

        dm_cheque_blank_card_number as (
            select
                cheque_pk,
                card_id,
                summ_discounted,
                datetime
            from {{ ref('cheque') }}
            where card_number is null
              and record_source = 'manzana'
        ),

        main_logic as (
            select distinct
                c.cheque_pk,
                c.summ_discounted
            from dm_cheque_blank_card_number c
            left join {{ ref('hub_cheque') }} sc on c.cheque_pk = sc.cheque_pk
            left join sat_card_del cd on c.card_id = cd.card_id
            left join {{ ref('card') }} cc on cc.card_id= c.card_id
            where rn = 1
              and cc.card_id is null
              and cd.deleted_flg = 1
              and c.datetime > cd.dt_load
              and sc.instance_id = cd.instance_id
        )

    select
        ch.cheque_pk                                                 as cheque_pk,
        case when ml.cheque_pk is not null then 'deleted_card' end   as reason_blank,
        ch.summ_discounted                                           as summ,
        '{{ var("dm_date") }}'::date                                 as dq_date_test,
        current_timestamp                                            as dq_load_datetime
    from dm_cheque_blank_card_number ch
    left join main_logic ml using(cheque_pk)

{% endmacro %}
