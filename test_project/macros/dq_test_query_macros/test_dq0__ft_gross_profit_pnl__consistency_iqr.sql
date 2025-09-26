{% macro test_dq0__ft_gross_profit_pnl__consistency_iqr(model) %}
{% set model = '' %}

WITH month_aggregates AS (
        SELECT
            "month",
            SUM(cm_amt) AS sum_cm_amt,
            SUM(fm_amt) AS sum_fm_amt,
            SUM(bm_amt) AS sum_bm_amt,
            SUM(sale_qty) AS sum_sale_qty
        FROM {{ ref('ft_gross_profit_pnl') }}
        GROUP BY "month"
    ),
    rolling_stats AS (
        SELECT
            m1."month",
            m1.sum_cm_amt, m1.sum_fm_amt, m1.sum_bm_amt, m1.sum_sale_qty,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY m2.sum_cm_amt) AS q1_cm_amt,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY m2.sum_cm_amt) AS q3_cm_amt,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY m2.sum_fm_amt) AS q1_fm_amt,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY m2.sum_fm_amt) AS q3_fm_amt,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY m2.sum_bm_amt) AS q1_bm_amt,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY m2.sum_bm_amt) AS q3_bm_amt,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY m2.sum_sale_qty) AS q1_sale_qty,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY m2.sum_sale_qty) AS q3_sale_qty
        FROM month_aggregates m1
        JOIN month_aggregates m2 ON ABS(m1."month" - m2."month") <= 3  -- Ограничение на 3 месяца
        GROUP BY m1."month", m1.sum_cm_amt, m1.sum_fm_amt, m1.sum_bm_amt, m1.sum_sale_qty
    ),
    bounds AS (
        SELECT
            "month",
            q1_cm_amt - 1.5 * (q3_cm_amt - q1_cm_amt) AS lower_cm_amt,
            q3_cm_amt + 1.5 * (q3_cm_amt - q1_cm_amt) AS upper_cm_amt,
            q1_fm_amt - 1.5 * (q3_fm_amt - q1_fm_amt) AS lower_fm_amt,
            q3_fm_amt + 1.5 * (q3_fm_amt - q1_fm_amt) AS upper_fm_amt,
            q1_bm_amt - 1.5 * (q3_bm_amt - q1_bm_amt) AS lower_bm_amt,
            q3_bm_amt + 1.5 * (q3_bm_amt - q1_bm_amt) AS upper_bm_amt,
            q1_sale_qty - 1.5 * (q3_sale_qty - q1_sale_qty) AS lower_sale_qty,
            q3_sale_qty + 1.5 * (q3_sale_qty - q1_sale_qty) AS upper_sale_qty
        FROM rolling_stats
    ),
    test AS (
    SELECT
        m."month",
        sum_cm_amt,
        b.lower_cm_amt AS lower_cm_amt,
        b.upper_cm_amt AS upper_cm_amt,
        sum_fm_amt,
        b.lower_fm_amt AS lower_fm_amt,
        b.upper_fm_amt AS upper_fm_amt,
        sum_bm_amt,
        b.lower_bm_amt AS lower_bm_amt,
        b.upper_bm_amt AS upper_bm_amt,
        sum_sale_qty,
        b.lower_sale_qty AS lower_sale_qty,
        b.upper_sale_qty AS upper_sale_qty,
        CASE WHEN sum_cm_amt < b.lower_cm_amt OR sum_cm_amt > b.upper_cm_amt THEN 'OUTLIER' ELSE 'OK' END AS cm_amt_status,
        CASE WHEN sum_fm_amt < b.lower_fm_amt OR sum_fm_amt > b.upper_fm_amt THEN 'OUTLIER' ELSE 'OK' END AS fm_amt_status,
        CASE WHEN sum_bm_amt < b.lower_bm_amt OR sum_bm_amt > b.upper_bm_amt THEN 'OUTLIER' ELSE 'OK' END AS bm_amt_status,
        CASE WHEN sum_sale_qty < b.lower_sale_qty OR sum_sale_qty > b.upper_sale_qty THEN 'OUTLIER' ELSE 'OK' END AS sale_qty_status
    FROM month_aggregates m
    JOIN bounds b ON m."month" = b."month"
    )
SELECT *,
    '{{ var("dm_date") }}'::date as dq_date_test,
    current_timestamp as dq_load_datetime
FROM test
WHERE
    (cm_amt_status <> 'OK' OR
    fm_amt_status <> 'OK' OR
    bm_amt_status <> 'OK' OR
    sale_qty_status <> 'OK'
    )
  AND "month" = extract('year' from current_date) * 100 + extract('month' from current_date)  -- проверяем только текущий месяц
{% endmacro %}