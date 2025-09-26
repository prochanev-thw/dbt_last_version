{% macro test_dq0__cheque__deleted(model) %}
{% set model = '' %}

SELECT
    dmc.cheque_pk,
    '{{ var("dm_date") }}'::date  as dq_date_test,
    current_timestamp             as dq_load_datetime
FROM {{ ref('cheque') }} dmc
JOIN (SELECT cheque_pk FROM (
    SELECT
    cheque_pk,
    deleted_flg,
    row_number() over (PARTITION BY cheque_pk ORDER BY version_id desc) rn
    FROM dds.sat_cheque
    ) c
WHERE rn = 1 and deleted_flg = 1) sc USING (cheque_pk)
WHERE record_source = 'manzana'

{% endmacro %}


