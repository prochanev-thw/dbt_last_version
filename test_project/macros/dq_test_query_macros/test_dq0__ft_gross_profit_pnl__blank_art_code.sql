{% macro test_dq0__ft_gross_profit_pnl__blank_art_code(model) %}
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
where art_code is null

{% endmacro %}