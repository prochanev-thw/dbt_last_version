{% macro test_dq0__ft_gross_profit_pnl__blank_month(model) %}
{% set model = '' %}

select art_code,
aum_sale_qty,
bm_amt,
cm_amt,
fm_amt,
frmt,
month,
sale_qty,
smoothed_bm_amt,
'{{ var("dm_date") }}'::date as dq_date_test,
current_timestamp as dq_load_datetime
from {{ ref('ft_gross_profit_pnl') }}
where month is null

{% endmacro %}