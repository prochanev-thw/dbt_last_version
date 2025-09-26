-- get partition stats by batch table
{% macro gather_stats_by_part(src_tab, tgt_tab, trunc_clause) %}
{# функция анализиирует список табличных имен партиций в src_tab (батч) по полю партиционирования в tgt_tab и собирает статистику по ним в tgt_tab
    Параметры: 
        src_tab - исходная таблица для анализа (батч)
        tgt_tab - конечная таблица (витрина)
        trunc_clause - необязательное имя анализируемого поля из src_tab или выражение трансформации для него
#}
    {% if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] and load_relation(tgt_tab) is not none %}
        {% set prt_type = get_partition_type(tgt_tab) %}
        {% set prt_col_type = get_partition_column_type(tgt_tab) %}
        {% if trunc_clause is none or trunc_clause is not defined %}
            {% set trunc_clause = get_partition_column(tgt_tab) %}
        {% endif %}
        {% set src_clause = get_batch_src_clause(src_tab, trunc_clause, prt_col_type) %}
        {{ gather_partition_stats_raw(src_clause, tgt_tab, prt_type, prt_col_type) }}
    {% endif %}
{%- endmacro %}