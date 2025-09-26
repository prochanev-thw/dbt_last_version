{% macro test_dq0__fct_trnsrcproducts__consistency_cheque_cheque_item_exist(model) %}
{% set model = '' %}


with cheque_number
as
(
select
     cheque_pk, 1 as exist

from
     {{ ref('cheque') }} c
where
     number is not null
	 and datetime between '{{ var("dm_date") }}'::timestamp - interval '32 days'
     and '{{ var("dm_date") }}'::timestamp - interval '1 days')


select
        ci.cheque_pk,
        datetime ::date as datetime,
        sum(summ_discounted) as summ_discounted,
       '{{ var("dm_date") }}'::date     as dq_date_test,
	    current_timestamp               as dq_load_datetime

from
        {{ ref('cheque_item') }} ci
left join cheque_number as cn on
        ci.cheque_pk = cn.cheque_pk
where
    datetime between '{{ var("dm_date") }}'::timestamp - interval '32 days'
    and '{{ var("dm_date") }}'::timestamp - interval '2 days'
 	and cn.exist is null
 	and ci.record_source = 'manzana'
group by ci.cheque_pk,
         datetime
{% endmacro %}
