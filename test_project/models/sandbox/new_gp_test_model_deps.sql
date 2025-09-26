{{
    config(
        materialized='table'
    )
}}

select *
from {{ ref('new_gp_test_model') }} # ignore_sensor