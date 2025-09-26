{%-
  /* Макрос создания таблицы с НЕуникальными записями.

  * model           (str): Имя модели dbt                                (обязательный)
  * keys            (str): Ключевые атрибуты через запятую одной строкой (обязательный)
  * predicate       (str или None): Предикат для условия where           (необязательный)
  * constanta_where (str или None): Константа для условия where          (необязательный)
  */
-%}

{% macro for_all__dq_mcrs__unique(model, keys='', predicate = None, constanta_where = None) %}

    {% set keys_list = keys.split(',') %}
    {% set keys_str = ', '.join(keys_list) %}

    {% set query_job %}
            select {{keys_str}},
               count(1) as cnt
            from {{ ref(model) }}
            {% if keys_list[0] and constanta_where %}
                where {{keys_list[0]}} {{ predicate }} '{{constanta_where}}'
            {% endif %}
            group by {{keys_str}}
            having count(1) > 1
    {% endset %}

    {{ query_job }}

{% endmacro %}


{% macro run_dq_query(query) %}
    {{ query }}
{% endmacro %}


{% macro adapter_run_dq_query(query) %}
    {{ adapter.execute(query, fetch='all') }}
{% endmacro %}



{% macro check_query_result(query) %}
    {% if execute %}
        {% set result_set = adapter.execute(query, fetch='all') %}
        {% do log(result_set, info=True) %}
    {% endif %}
    {{ return(result_set) }}
{% endmacro %}



{% macro raise_exc_if_table_has_rows(table_name) %}
  {%- if execute -%}
    {%- call statement('count_rows', fetch_result=True) -%}
      select count(*) AS row_count FROM {{ table_name }}
    {%- endcall -%}
    {% set results = load_result('count_rows') %}
    {% set row_count = results.data[0][0] %}
    {% if row_count == 0 %}
      {{log( 'Таблица с результатами теста ' ~ table_name ~' НЕ содержит строки' ,true)}}
    {% endif %}
    {% if row_count > 0 %}
      {{ exceptions.raise_compiler_error('!!!Таблица с результатами теста ' ~ table_name ~' СОДЕРЖИТ строки!!!') }}
    {% endif %}
  {%- endif -%}
{% endmacro %}
