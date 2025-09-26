{% macro test_dq0__fct_trnsrcproducts__consistency_cheque_cheque_item_month(model) %}
{% set model = '' %}


with cheque_item
as
(
select
	cheque_pk,
	sum(summ_discounted) as sum_cheque_item,
	datetime::date

from
    {{ ref('cheque_item') }} ci
where
	datetime >= to_date('{{ var("dm_date") }}', 'YYYY-MM-DD') - 32
group by
	cheque_pk,
	datetime)


select
	 c.cheque_id,
	summ_discounted sum_cheque,
	sum_cheque_item sum_cheque_item,
	c.datetime::date,
	'{{ var("dm_date") }}'::date  as dq_date_test,
	current_timestamp    as dq_load_datetime

from
	{{ ref('cheque') }}  c
join cheque_item on
	cheque_item.cheque_pk = c.cheque_pk
where
	c.datetime >= to_date('{{ var("dm_date") }}', 'YYYY-MM-DD') - 32
	and (summ_discounted != sum_cheque_item)
	and  abs(summ_discounted) - abs(sum_cheque_item) > 0.50
{% endmacro %}