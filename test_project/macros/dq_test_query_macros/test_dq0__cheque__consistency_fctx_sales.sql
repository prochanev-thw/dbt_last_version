{% macro test_dq0__cheque__consistency_fctx_sales(model) %}
{% set model = '' %}

    with
        khd_cheque as (
            select
                sal_transactionid,
                sal_transactiondate,
                sum(sal_m_discountloypoints) as summ
            from {{ ref('fctx_sales') }}
            where
                sal_m_discountloypoints > 0
                and sal_transactiondate between '{{ var("dm_date") }}'::timestamp - interval '32 days'
                                            and '{{ var("dm_date") }}'::timestamp - interval '2 days'
                and sal_transactiontype = 'SALE'
            group by sal_transactionid, sal_transactiondate
        ),

        manzana_cheque as (
            select
                "number"
            from {{ ref('cheque') }}
            where
                datetime between '{{ var("dm_date") }}'::timestamp - interval '62 days'
                             and '{{ var("dm_date") }}'::timestamp - interval '2 days'
            group by "number"
        )

    select
        khd.sal_transactionid         as sal_transactionid,
        khd.sal_transactiondate       as sal_transactiondate,
        khd.summ                      as summ,
        '{{ var("dm_date") }}'::date  as dq_date_test,
        current_timestamp             as dq_load_datetime
    from khd_cheque khd
    left join manzana_cheque mzn
        on khd.sal_transactionid = mzn."number"
    where mzn."number" is null

{% endmacro %}
