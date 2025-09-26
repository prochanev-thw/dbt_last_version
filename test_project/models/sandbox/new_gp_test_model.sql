{{
    config(
        materialized='table'
    )
}}

select *
from (
    values
        (1, 'row_01'),
        (2, 'row_02'),
        (3, 'row_03'),
        (4, 'row_04'),
        (5, 'row_05'),
        (6, 'row_06'),
        (7, 'row_07'),
        (8, 'row_08'),
        (9, 'row_09'),
        (10, 'row_10')
) as sandbox_rows(row_id, row_label)