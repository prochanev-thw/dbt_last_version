{% macro test_dq0__fct_trnsrcproducts__consistency_cheque_fctx_sales(model) %}
{% set model = '' %}


with t1 as (
select
	  				sal_transactiondate ,
	  				sal_transactionid,
	  			    sum(sales) 	as sum_sales
from
	(
	select
		sal_transactionid,
		sal_transactiondate,
		sal_m_value - coalesce(sal_m_discount,0) - coalesce(sal_m_discountloyprogs,0) - coalesce(sal_m_discountloypoints,0) 	as sales
	from
		{{ ref('fctx_sales') }} fs2
	where
		sal_transactiondate::date >= to_date('{{ var("dm_date") }}','YYYY-MM-DD') -32 and sal_transactiondate::date <= to_date('{{ var("dm_date") }}','YYYY-MM-DD') - 23) as t
group by
		 			    sal_transactionid,
		                sal_transactiondate

		                ),



	 t2 as (
select
	  				sal_transactiondate ,
	  				sal_transactionid,
	  			    sum(sales) as sum_sales
from
	(
	select
		sal_transactionid,
		sal_transactiondate,
		sal_m_value - coalesce(sal_m_discount,0) - coalesce(sal_m_discountloyprogs,0) - coalesce(sal_m_discountloypoints,0) as sales
	from
		{{ ref('fctx_sales') }} fs2
	where
		sal_transactiondate::date >= to_date('{{ var("dm_date") }}','YYYY-MM-DD') -22 and sal_transactiondate::date <= to_date('{{ var("dm_date") }}',
			'YYYY-MM-DD')-13) as t
group by
		 			    sal_transactionid,
		                sal_transactiondate

		                ),



	 t3 as (
select
	  				sal_transactiondate ,
	  				sal_transactionid,
	  			    sum(sales) as sum_sales
from
	(
	select
		sal_transactionid,
		sal_transactiondate,
		sal_m_value - coalesce(sal_m_discount,0) - coalesce(sal_m_discountloyprogs,0) - coalesce(sal_m_discountloypoints,0) as sales
	from
		{{ ref('fctx_sales') }} fs2
	where
		sal_transactiondate::date >= to_date('{{ var("dm_date") }}','YYYY-MM-DD') -12 and sal_transactiondate::date <= to_date('{{ var("dm_date") }}','YYYY-MM-DD')-2) as t
group by
		 			    sal_transactionid,
		                sal_transactiondate

		                ),




		cheque as (
select
					 cheque_pk,
					 number,
					 datetime :: date as datetime,
					 summ_discounted
from
				 	 {{ ref('cheque') }} c
where
	datetime >= to_date('{{ var("dm_date") }}','YYYY-MM-DD') -32 and datetime::date <= to_date('{{ var("dm_date") }}','YYYY-MM-DD')-2)



select
		cheque_pk,
		number,
	    datetime,
		round(abs(summ_discounted)-abs(sum_sales),2) as difference,
		'{{ var("dm_date") }}'::date                 as dq_date_test,
	    current_timestamp                            as dq_load_datetime
from
	    cheque as c
join t1 as t1 on
		t1.sal_transactionid = c.number
where
		abs(summ_discounted)-abs(sum_sales) > 0.5
union
select
		cheque_pk,
		number,
	    datetime,
		round(abs(summ_discounted)-abs(sum_sales),2) as difference,
		'{{ var("dm_date") }}'::date                 as dq_date_test,
	    current_timestamp                            as dq_load_datetime
from
	    cheque as c
join t2 as t2 on
		t2.sal_transactionid = c.number
where
		abs(summ_discounted)-abs(sum_sales) > 0.5
union
select
		cheque_pk,
		number,
	    datetime,
		round(abs(summ_discounted)-abs(sum_sales),2) as difference,
		'{{ var("dm_date") }}'::date                 as dq_date_test,
	    current_timestamp                            as dq_load_datetime
from
	    cheque as c
join t3 as t3 on
		t3.sal_transactionid = c.number
where
		abs(summ_discounted)-abs(sum_sales) > 0.5
{% endmacro %}
