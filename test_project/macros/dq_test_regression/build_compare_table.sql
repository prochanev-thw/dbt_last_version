-- сравнивает 2 таблицы с одинаковыми ключами целиком, либо на определённых промежутках
{% macro build_compare_table(first_table_schema,
                    first_table_name,
                    second_table_schema,
                    second_table_name,
                    tables_pk,
                    columns,
                    first_table_where_condition='',
                    second_table_where_condition=''
                    ) %}
    {% set res = drop_table('sandbox', 'compare_' + first_table_name + '_' + second_table_name) %}

    {% set key_list = tables_pk.split(',')%}
     
    {% set create_compare_query %}
        create table sandbox.compare_{{ first_table_name }}_{{ second_table_name }} as
            with table_1 as (
                select
                    *
                from {{ first_table_schema }}.{{ first_table_name }}
                where {{ first_table_where_condition }}
            ),
            table_2 as (
                select
                    *
                from {{ second_table_schema }}.{{ second_table_name }}
                where {{ second_table_where_condition }}
            )
            select
                {%- for column in columns %}
                    t1.{{ column }} as t1_{{ column }},
                {%- endfor %}
                {%- for column in columns %}
                    t2.{{ column }} as t2_{{ column }},
                {%- endfor %}
                current_timestamp as load_datetime
            from table_1 as t1
            inner join table_2 as t2
                on 
                {%- for key_ in key_list %}
                    t1.{{ key_ }} = t2.{{ key_ }}
                    {% if not loop.last %}
                    and 
                    {% endif %}
                {%- endfor %}
    {% endset %}

    {% set res = run_query(create_compare_query) %}

    {{ return(res) }}

{%- endmacro %}

{% macro build_compare_result_table(first_table_name, second_table_name) %}
    {% set res = drop_table('sandbox', 'compare_result_' + first_table_name + '_' + second_table_name) %}

    {% set query %}
        create table sandbox.compare_result_{{ first_table_name }}_{{ second_table_name }} (
            column_nm varchar(60),
            first_table_cnt int8,
            second_table_cnt int8,
            first_table_null_cnt int8,
            first_table_null_pct varchar(11),
            second_table_null_cnt int8,
            second_table_null_pct varchar(11),
            null_diff_cnt int8,
            null_diff_pct varchar(11),
            diff_cnt int8,
            diff_pct varchar(11)
    )
    {% endset %}

    {% do run_query(query) %}

    {{log("Final table name sandbox.compare_result_" + first_table_name + "_" + second_table_name, true)}}
{%- endmacro %} 

{% macro insert_key_info(first_table_schema, 
                        first_table_name, 
                        second_table_schema, 
                        second_table_name, 
                        tables_pk,
                        first_table_where_condition="",
                        second_table_where_condition="") %}
    {% set key_list = tables_pk.split(',')%}

    {% set count_query %}
        with table_1 as (
            select
                {%- for key_ in key_list %}
                    {{ key_ }},
                {%- endfor %}
                1
            from {{ first_table_schema }}.{{ first_table_name }}
            where {{ first_table_where_condition }}
        ),
        table_2 as (
            select
                {%- for key_ in key_list %}
                    {{ key_ }},
                {%- endfor %}
                1
            from {{ second_table_schema }}.{{ second_table_name }}
            where {{ second_table_where_condition }}
        )
        select
            0 as diff_count,
            {%- for key_ in key_list %}
                'tmp' as first_table_{{ key_ }},
            {%- endfor %}
            {%- for key_ in key_list %}
                'tmp' as second_table_{{ key_ }},
            {%- endfor %}
            1 as tmp
        union all
        select
            count(*) over () as diff_count,
        {%- for key_ in key_list %}
            t1.{{ key_ }}::varchar as first_table_{{ key_ }},
        {%- endfor %}
        {%- for key_ in key_list %}
            t2.{{ key_ }}::varchar as second_table_{{ key_ }},
        {%- endfor %}
        1 as tmp
        from table_1 as t1
        full join table_2 as t2
            on 
            {%- for key_ in key_list %}
                t1.{{ key_ }} = t2.{{ key_ }}
                {% if not loop.last %}
                and 
                {% endif %}
            {%- endfor %}
        where 
            {%- for key_ in key_list %}
                t1.{{ key_ }} is NULL or t2.{{ key_ }} is NULL
                {% if not loop.last %}
                or 
                {% endif %}
            {%- endfor %}
        limit 5
    {% endset %}

   {% set first_cnt_query %}
        select
            count(1) as first_cnt
        from {{ first_table_schema }}.{{ first_table_name }}
        where {{ first_table_where_condition }}
    {% endset %}

    {% set second_cnt_query %}
        select
            count(1) as second_cnt
        from {{ second_table_schema }}.{{ second_table_name }}
        where {{ second_table_where_condition }}
    {% endset %}

    {% set diff_result =  dbt_utils.get_query_results_as_dict(count_query) %}
    {% set diff_cnt_result = diff_result['diff_count'][-1] %}
    {% set diff_cnt_result = 0 %}
    {% set first_cnt_result = dbt_utils.get_query_results_as_dict(first_cnt_query)['first_cnt'][0] %}
    {% set second_cnt_result = dbt_utils.get_query_results_as_dict(second_cnt_query)['second_cnt'][0] %}   

    {% set final_query %}
        insert into sandbox.compare_result_{{ first_table_name }}_{{ second_table_name }}
        select 
            'primary_keys' as column_nm,
            {{ first_cnt_result }} as first_table_cnt,
            {{ second_cnt_result }} as second_table_cnt,
            0 as first_table_null_cnt,
            0.0 as first_table_null_pct,
            0 as second_table_null_cnt,
            0.0 as second_table_null_pct,
            0 as null_diff_cnt,
            0.0 as null_diff_pct,
            {{ diff_cnt_result }} as diff_cnt,
            concat((round({{ diff_cnt_result if diff_cnt_result is not none else 0 }}::numeric / ({{ first_cnt_result }} + {{ second_cnt_result }})::numeric, 1) * 100)::varchar(10), '%') as diff_pct
    {% endset %}

    {% do run_query(final_query) %}

{%- endmacro %} 

{% macro insert_column_info(first_table_schema, 
                        first_table_name, 
                        second_table_schema, 
                        second_table_name, 
                        tables_pk,
                        column,
                        first_table_where_condition='',
                        second_table_where_condition='') %}
    
    {% set key_list = tables_pk.split(',') %}

    {% set first_cnt_query %}
        select
            count(1) as first_cnt
        from {{ first_table_schema }}.{{ first_table_name }}
        where {{ first_table_where_condition }}
    {% endset %}

    {% set second_cnt_query %}
        select
            count(1) as second_cnt
        from {{ second_table_schema }}.{{ second_table_name }}
        where {{ second_table_where_condition }}
    {% endset %}

    {% set first_cnt_null_query %}
        select
            count(1) as first_not_null_cnt
        from {{ first_table_schema }}.{{ first_table_name }}
        where ({{ first_table_where_condition }})
            and {{ column }} is NULL
    {% endset %}

    {% set second_cnt_null_query %}
        select
            count(1) as second_not_null_cnt
        from {{ second_table_schema }}.{{ second_table_name }}
        where ({{ second_table_where_condition }})
            and {{ column }} is NULL
    {% endset %}

    {% set diff_query %}
        select
            count(1) as diff_cnt
        from sandbox.compare_{{ first_table_name }}_{{ second_table_name }}
        where t1_{{ column }} != t2_{{ column }}
    {% endset %}

    {% set diff_query =  dbt_utils.get_query_results_as_dict(diff_query)['diff_cnt'][0] %}
    {% set first_null_cnt_result =  dbt_utils.get_query_results_as_dict(first_cnt_null_query)['first_not_null_cnt'][0] %}
    {% set second_null_cnt_result =  dbt_utils.get_query_results_as_dict(second_cnt_null_query)['second_not_null_cnt'][0] %}
    {% set first_cnt_result =  dbt_utils.get_query_results_as_dict(first_cnt_query)['first_cnt'][0] %}
    {% set second_cnt_result =  dbt_utils.get_query_results_as_dict(second_cnt_query)['second_cnt'][0] %}

    {% set final_query %}
        insert into sandbox.compare_result_{{ first_table_name }}_{{ second_table_name }}
        select 
            '{{ column }}' as column_nm,
            {{ first_cnt_result }} as first_table_cnt,
            {{ second_cnt_result }} as second_table_cnt,
            {{ first_null_cnt_result }} as first_table_null_cnt,
            concat(round({{ first_null_cnt_result }}::numeric / {{ first_cnt_result }}::numeric * 100, 1)::varchar(10), '%') as first_table_null_pct,
            {{ second_null_cnt_result }} as second_table_null_cnt,
            concat(round({{ second_null_cnt_result }}::numeric / {{ second_cnt_result }}::numeric * 100, 1)::varchar(10), '%') as second_table_null_pct,
            abs({{ second_null_cnt_result }} - {{ first_null_cnt_result }} ) as null_diff_cnt,
            concat((coalesce(round(abs({{ second_null_cnt_result }} - {{ first_null_cnt_result }} )::numeric / {{ first_cnt_result }}::numeric, 1), 0) * 100)::varchar(10), '%') as null_diff_pct,
            coalesce({{ diff_query }}, 0) as diff_cnt,
            concat((coalesce(round({{ diff_query }}::numeric / {{ first_cnt_result }}::numeric, 1), 0) * 100)::varchar(10), '%') as diff_pct
    {% endset %}

    {% do run_query(final_query) %}

{%- endmacro %} 

{% macro clean_all_tmp_tables(first_table_name, second_table_name) %}
    {% set res =  drop_table('sandbox', 'compare_'+first_table_name + '_' + second_table_name)%}

{%- endmacro %}
