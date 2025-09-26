{% macro test_dq0__bonus_addition__consistency_cheque_del(model) %}
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
                cheque_pk <> '' and
                created_on between '{{ var("dm_date") }}'::date - interval '1 month'
                               and '{{ var("dm_date") }}'::date - interval '2 day'
        ),
        s_chq_rn as(
            select
                row_number() over (partition by cheque_pk order by version_id desc) as rn,
                cheque_pk,
                deleted_flg
            from {{ ref('sat_cheque') }}
        ),
        s_chq as(
            select *
            from s_chq_rn
            where
                rn = 1 and
                deleted_flg = 1
        )
    --Бонус addition не удален, чек удален: кол-во
    select
        bns.created_on                as created_on,
        bns.cheque_pk                 as cheque_pk,
        bns.bonus_pk                  as bonus_pk,
        bns.cheque_id                 as cheque_id,
        bns.bonus_id                  as bonus_id,
        bns.instance_id               as instance_id,
        'bonus_not_d__cheque_d'::varchar       as dq_result_status,
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from bns
    inner join s_chq on bns.cheque_pk = s_chq.cheque_pk

{% endmacro %}