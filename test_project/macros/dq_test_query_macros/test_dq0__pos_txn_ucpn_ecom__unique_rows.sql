{% macro test_dq0__pos_txn_ucpn_ecom__unique_rows(model) %}
{% set model = '' %}

    WITH count_rows AS (
        SELECT txn_save_dt, COUNT(1) AS rows_per_day
        FROM dm.pos_txn_ucpn_ecom
        WHERE txn_save_dt >= '{{ var("dm_date") }}'::DATE - INTERVAL '14 days'
        GROUP BY txn_save_dt
        ORDER BY txn_save_dt
    ), 
    check_todays_rows AS (
        SELECT 
            CASE 
                WHEN cr.rows_per_day IS NULL THEN 1 
                ELSE 0 
            END AS todays_rows_is_empty
        FROM (SELECT '{{ var("dm_date") }}'::DATE AS txn_save_dt) d  
        LEFT JOIN count_rows cr ON d.txn_save_dt = cr.txn_save_dt
    ), 
    mediana_14_days AS (
        SELECT median(rows_per_day) AS mediana_14_days
        FROM count_rows
        WHERE txn_save_dt != '{{ var("dm_date") }}'::DATE
    ), 
    filtered_rows AS (
        SELECT 
            cr.txn_save_dt,
            cr.rows_per_day,
            m.mediana_14_days,
            c.todays_rows_is_empty
        FROM count_rows cr
        CROSS JOIN mediana_14_days m
        CROSS JOIN check_todays_rows c
        WHERE cr.txn_save_dt >= '{{ var("dm_date") }}'::DATE - INTERVAL '7 days'
          AND cr.txn_save_dt < '{{ var("dm_date") }}'::DATE
          AND cr.rows_per_day < 0.1 * m.mediana_14_days
    )
    SELECT 
                COALESCE(STRING_AGG(concat(txn_save_dt, ' : ', rows_per_day), ', '), '0') AS anomaly_result,
                COALESCE(MAX(mediana_14_days), 0)                                        AS mediana_14_days,
                COALESCE(MAX(mediana_14_days) * 0.1 , 0)                                AS mediana_anomaly_level,
                (SELECT todays_rows_is_empty FROM check_todays_rows)                    AS todays_rows_is_empty,
                 '{{ var("dm_date") }}'::date                    as dq_date_test,
                 current_timestamp                               as dq_load_datetime
    FROM filtered_rows
    
    




{% endmacro %}

