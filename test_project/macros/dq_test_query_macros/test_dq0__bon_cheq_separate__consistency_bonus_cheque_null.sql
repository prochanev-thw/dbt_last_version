{% macro test_dq0__bon_cheq_separate__consistency_bonus_cheque_null(model) %}
{% set model = '' %}

    with
        bns as(
            select
                created_on,
                cheque_pk,
                bonus_pk,
                cheque_id,
                bonus_id,
                instance_id
            from {{ ref('bonus_addition') }}
            where
                record_source = 'manzana' and
                cheque_pk not in (select cheque_pk from dict.fake_cheque) and
                created_on between '{{ var("dm_date") }}'::date - interval '1 month'
                               and '{{ var("dm_date") }}'::date - interval '2 day'
        ),
        dm_chq as (
            select cheque_pk
            from {{ ref('cheque') }}
            where datetime >='2022-11-01'
                and record_source = 'manzana'
        )

    --Бонус addition пришел, а чек не пришел: кол-во
    select
        bns.created_on                   as created_on,
        bns.cheque_pk                    as cheque_pk,
        bns.bonus_pk                     as bonus_pk,
        bns.cheque_id                    as cheque_id,
        bns.bonus_id                     as bonus_id,
        bns.instance_id                  as instance_id,
        'bonus_not_null__cheque_is_null'::varchar as dq_result_status,
        '{{ var("dm_date") }}'::date     as dq_date_test,
        current_timestamp                as dq_load_datetime
    from bns
    left join dm_chq on bns.cheque_pk = dm_chq.cheque_pk
    where dm_chq.cheque_pk is null

{% endmacro %}