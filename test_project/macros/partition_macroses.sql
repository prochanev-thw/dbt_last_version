{% macro get_batch_src_clause(src_tab, column_name, column_type) %}
{# функция возвращает код запроса для определения данныых партиционирования в батче #}
    {%- set query -%}
        select distinct {{column_name}}::{{column_type}} as val from {{src_tab}}
    {%- endset -%}
    {{return(query)}}
{% endmacro %}

{% macro get_val_src_clause(val_list, column_type) %}
{# функция возвращает код запроса для определения данныых партиционирования из списка значений #}
    {%- set query -%}
        {%- for val in val_list -%}
            select '{{val}}'::{{column_type}} as val {% if not loop.last -%} union {% endif -%}
        {%- endfor -%}
    {%- endset -%}
    {{return(query)}}
{% endmacro %}

{% macro get_partition_type(relation) %}
{# функция возвращает тип партиционирования (list/range) в таблице relation #}
    {%- set query -%}
        select max(p.partitiontype)
        from pg_catalog.pg_partitions p
        where p.schemaname = '{{relation.schema}}' and p.tablename = '{{relation.table}}'
    {%- endset -%}
    {% set prt_type = dbt_utils.get_single_value(query) %}
    {{return(prt_type)}}
{% endmacro %}

{% macro get_partition_column(relation) %}
{# функция возвращает имя колонки партиционирования в таблице relation #}
    {%- set query -%}
        select p.columnname
        from pg_partition_columns p
        where p.schemaname = '{{relation.schema}}' and p.tablename = '{{relation.table}}'
    {%- endset -%}
    {% set prt_col = dbt_utils.get_single_value(query) %}
    {{return(prt_col)}}
{% endmacro %}

{% macro get_partition_column_type(relation) %}
{# функция возвращает тип колонки партиционирования в таблице relation #}
    {%- set query -%}
        select c.udt_name
        from information_schema.columns c
        join pg_partition_columns p on
        p.schemaname = c.table_schema and p.tablename = c.table_name and p.columnname = c.column_name
        where c.table_schema = '{{relation.schema}}' and c.table_name = '{{relation.table}}'
    {%- endset -%}
    {% set prt_col_type = dbt_utils.get_single_value(query) %}
    {{return(prt_col_type)}}
{% endmacro %}

{% macro get_partition_min_range_start(relation, partition_column_type) %}
{# функция возвращает минимальное начало диапазона партиционирования
    Параметры: 
        relation - анализиаруемая таблица
        partition_column_type - тип данных поля партиционирования
#}
    {%- set query -%}
        select min(nullif(split_part(p.partitionrangestart,'::',1),'')::{{partition_column_type}})::text
        from pg_catalog.pg_partitions p
        where p.partitiontype='range' and p.schemaname = '{{relation.schema}}' and p.tablename = '{{relation.table}}'
    {%- endset -%}
    {% set range_start = dbt_utils.get_single_value(query) %}
    {{return(range_start)}}
{% endmacro %}

{% macro get_partition_range(relation, partition_column_type) %}
{# функция возвращает диапазон партиционирования в range-таблице relation
    сначала функция пытается найти тот набор диапазонов, что был задан при создании таблицы через выражение every.
    если партиции всегда создавались вручную и таковой не нашелся (в fctx_sales, к примеру),
    то анализируется самый последний добавленный период в таблице и решение о диапазоне считается на его основе.
    дла таблиц, партицированных по полю времени возвращается day/week/mon/year,
    дла таблиц, партицированных по числовому полю возвращается число
    Параметры: 
        relation - анализиаруемая таблица
        partition_column_type - тип данных поля партиционирования
#}
    {%- set query -%}
        select nullif(trim('''' from split_part(max(p.partitioneveryclause),'::',1)),'')
        from pg_catalog.pg_partitions p
        where p.schemaname = '{{relation.schema}}' and p.tablename = '{{relation.table}}'
    {%- endset -%}
    {% set prt_range = dbt_utils.get_single_value(query) %}

    {% if prt_range is none %}
        {% if partition_column_type in ['date', 'timestamp', 'timestamptz'] %}
            {% set main_clause = "age(a.range_end, a.range_start)" %}
            {% set start_clause = "trim('''' from split_part(p.partitionrangestart,'::',1))::timestamp" %}
            {% set end_clause = "trim('''' from split_part(p.partitionrangeend,'::',1))::timestamp" %}
        {% elif partition_column_type in ['int2', 'int4', 'int8', 'float4', 'float8', 'numeric'] %}
            {% set main_clause = "nullif(trim('''' from split_part(max(p.partitioneveryclause),'::',1)),'')" %}
            {% set start_clause = "p.partitionrangestart" %}
            {% set end_clause = "p.partitionrangeend" %}
        {% else %}
            {{log("data type " ~ partition_column_type ~ " is unusable for range detection",true)}}
            {{return(none)}}
        {% endif %}

        {%- set query -%}
            select ({{main_clause}})::varchar
            from (
            select {{start_clause}} as range_start,
                {{end_clause}} as range_end,
                row_number() over (partition by p.schemaname, p.tablename order by p.partitionposition desc) as rn
            from pg_catalog.pg_partitions p
            where p.partitiontype='range'
            and p.schemaname = '{{relation.schema}}'
            and p.tablename = '{{relation.table}}') a
            where a.rn = 1
        {%- endset -%}
        {% set prt_range = dbt_utils.get_single_value(query) %}
    {% endif %}

    {% if prt_range.split(" ")[1] is undefined %}
        {{return(prt_range)}}
    {% elif prt_range.split(" ")[2] is defined %}
        {{log("range value " ~ prt_range ~ " is unsupported",true)}}
        {{return(none)}}
    {% elif prt_range.split(" ")[0] == '1'  %}
        {{return(prt_range.split(" ")[1])}}
    {% elif prt_range == '7 days' %}
        {{return("week")}}
    {% endif %}

    {{log("range value " ~ prt_range ~ " is unsupported",true)}}
    {{return(none)}}
{% endmacro %}

{% macro get_partition_names(src_clause, tgt_tab, partition_type, partition_column_type) %}
{# функция выдает список табличных имен партиций в tgt_tab по значениям из выражения в src_clause 
    Параметры: 
        src_clause - выражение для получния исходных данных для партиционирования
        tgt_tab - конечная таблица (витрина)
        partition_range - тип партиционирования (range/list)
        partition_column_type - тип поля партиционирования
#}
    {% if partition_type == 'list' %}
        {% set start_clause = "trim('''' from split_part(p.partitionlistvalues,'::',1))" %}
        {% set end_clause = "trim('''' from split_part(p.partitionlistvalues,'::',1))" %}
        {% set query %}
            select distinct p.partitionschemaname||'.'||p.partitiontablename as partition_name,
                {{start_clause}} as range_start,
                {{end_clause}} as range_end
            from ({{src_clause}}) b
            left join pg_catalog.pg_partitions p
                on p.schemaname = '{{tgt_tab.schema}}'
                and p.tablename = '{{tgt_tab.table}}'
                and b.val = trim('''' from split_part(p.partitionlistvalues,'::',1))::{{partition_column_type}}
                and p.partitionisdefault = false
            where p.partitionschemaname is not null
                and p.partitionschemaname not ilike '%history%'
        {% endset %}
    {% else %}
        {% set start_clause = "trim('''' from split_part(p.partitionrangestart,'::',1))" %}
        {% set end_clause = "trim('''' from split_part(p.partitionrangeend,'::',1))" %}
        {% set query %}
            with prt_tab as (
                select p.partitionschemaname||'.'||p.partitiontablename as partition_name,
                    {{start_clause}}::{{partition_column_type}} as range_start,
                    {{end_clause}}::{{partition_column_type}} as range_end,
                    p.partitionstartinclusive,
                    p.partitionendinclusive
                from pg_catalog.pg_partitions p
                where p.schemaname = '{{tgt_tab.schema}}'
                and p.tablename = '{{tgt_tab.table}}'
                and p.partitiontablename not ilike '%history%')
            select distinct p.partition_name,
                p.range_start::varchar as range_start,
                p.range_end::varchar as range_end
            from ({{src_clause}}) b
            join prt_tab p
                on case when p.partitionstartinclusive is true then b.val >= p.range_start
                    else b.val > p.range_start end
                and case when p.partitionendinclusive is true then b.val <= p.range_end
                    else b.val < p.range_end end
        {% endset %}
    {% endif %}
    {% set prt_name = run_query(query) %}
    {{return(prt_name)}}
{%- endmacro %}

{% macro get_missing_list_partition(src_clause, tgt_tab, partition_column_type) %}
{# функция возвращает набор списочных значений из выражения src_clause для которых отсутствуют партиции в tgt_tab
    Параметры: 
        src_clause - выражение для получния исходных данных для партиционирования
        tgt_tab - конечная таблица (витрина)
        partition_column_type - тип поля партиционирования
#}
    {% set query %}
        select distinct b.val::varchar
        from ({{src_clause}}) b
        left join pg_catalog.pg_partitions p
            on p.schemaname = '{{tgt_tab.schema}}'
            and p.tablename = '{{tgt_tab.table}}'
            and b.val = trim('''' from split_part(p.partitionlistvalues,'::',1))::{{partition_column_type}}
            and p.partitionisdefault = false
        where p.partitionname is null
    {% endset %}
    {% set results = run_query(query) %}
    {{ return(results) }}
{% endmacro %}


{% macro get_missing_range_partition(src_clause, tgt_tab, partition_range, partition_column_type) %}
{# функция возвращает набор диапазонных значений из выражения src_clause для которых отсутствуют партиции в tgt_tab
    Параметры: 
        src_clause - выражение для получения списка значений партиционирования
        tgt_tab - конечная таблица (витрина)
        partition_range - диапазон партиционирования для таблицы
        partition_column_type - тип поля партиционирования
#}
    {% if partition_column_type in ['date', 'timestamp', 'timestamptz'] %}
        {% set start_clause = "nullif(trim('''' from split_part(p.partitionrangestart,'::',1)),'')" %}
        {% set end_clause = "nullif(trim('''' from split_part(p.partitionrangeend,'::',1)),'')" %}
        {% set part_start = "date_trunc('" ~ partition_range ~ "', b.val)" %}
        {% set part_end = "date_trunc('" ~ partition_range ~ "', b.val) + interval '1 " ~ partition_range ~ "'"  %}
    {% elif partition_column_type in ['int2', 'int4', 'int8', 'float4', 'float8', 'numeric'] %}
        {% set min_range = get_partition_min_range_start(tgt_tab, partition_column_type) %}
        {% set start_clause = "nullif(p.partitionrangestart,'')" %}
        {% set end_clause = "nullif(p.partitionrangeend,'')" %}
        {% set part_start = min_range ~ "+(round(val/" ~ partition_range ~ ")*" ~ partition_range ~ ")" %}
        {% set part_end = min_range ~ "+(round(val/" ~ partition_range ~ ")*" ~ partition_range ~ ")+"~  partition_range %}
    {% else %}
        {{log("range type " ~ partition_column_type ~ " is unsupported",true)}}
        {{return(none)}}
    {% endif %}

    {% set query %}
        with prt_tab as (select {{start_clause}}::{{partition_column_type}} as range_start,
                    {{end_clause}}::{{partition_column_type}} as range_end,
                    p.partitionstartinclusive,
                    p.partitionendinclusive
                from pg_catalog.pg_partitions p
                where p.schemaname = '{{tgt_tab.schema}}'
                and p.tablename = '{{tgt_tab.table}}')
        select distinct ({{part_start}})::varchar as part_start,
            ({{part_end}})::varchar as part_end
        from ({{src_clause}}) b
        left join prt_tab p
            on case when p.partitionstartinclusive is true then b.val >= p.range_start
                else b.val > p.range_start end
            and case when p.partitionendinclusive is true then b.val <= p.range_end
                else b.val < p.range_end end
        where p.partitionstartinclusive is null
    {% endset %}
    {% set results = run_query(query) %}
    {{ return(results) }}
{% endmacro %}

{% macro add_missing_partitions(src_clause, tgt_tab, partition_type, partition_column_type) %}
{# функция ищет значения партиционирования из src_clause и создает партиции в tgt_tab, если их там нет
    Параметры: 
        src_clause - выражение для получения списка значений партиционирования
        tgt_tab - конечная таблица (витрина)
        partition_type - тип партиционирования (range/list)
        partition_column_type - тип данных поля партиционирования
#}
    {% if partition_type is none or partition_column_type is none %}
        {{ log("no partitioning data for  " ~ tgt_tab ~ ", exiting", true) }}
        {{ return("") }}
    {% endif %}
    {% if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] and load_relation(tgt_tab) is not none %}
        {% if partition_type == 'list' %}
            {% set results = get_missing_list_partition(src_clause, tgt_tab, partition_column_type) %}
            {% set query %}
                select
                    p.partitionname
                from pg_catalog.pg_partitions p
                where p.schemaname = '{{tgt_tab.schema}}'
                    and p.tablename = '{{tgt_tab.table}}'
                    and p.partitionisdefault = true
            {% endset %}
            {% set default_part = run_query(query)[0] %}
            {% if not default_part %}
                {% for val in results %}
                    {% set query %}
                        alter table {{tgt_tab}} add partition m{{val[0]}} values ('{{val[0]}}'::{{partition_column_type}})
                        with (appendonly='true', blocksize='32768', compresstype=zstd, compresslevel='4', orientation='column')
                    {% endset %}
                    {{log("Adding missing partition for value: " ~ val[0] ~ " for table " ~ tgt_tab,true)}}
                    {% do run_query(query) %}
                {% endfor %}
                {% if results|length > 0 %}
                    {{log("Adding missing partition done!",true)}}
                {% endif %}
            {% elif default_part %}
                {% for val in results %}
                    {% set query %}
                        alter table {{tgt_tab}} split default partition at('{{val[0]}}'::{{partition_column_type}}) into (partition m{{val[0]}}, default partition)
                    {% endset %}
                    {{log("Adding missing partition for value: " ~ val[0] ~ " for table " ~ tgt_tab,true)}}
                    {% do run_query(query) %}
                {% endfor %}
                {% if results|length > 0 %}
                    {{log("Adding missing partition done!",true)}}
                {% endif %}
            {% endif %}
        {% elif partition_type == 'range' %}
            {% set results = get_missing_range_partition(src_clause, tgt_tab, get_partition_range(tgt_tab, partition_column_type), partition_column_type) %}
            {% for val in results %}
                {% set query %}
                    alter table {{tgt_tab}} add partition start ('{{ val[0] }}'::{{partition_column_type}}) inclusive end ('{{ val[1] }}'::{{partition_column_type}}) exclusive
                    with (appendonly='true', blocksize='32768', compresstype=zstd, compresslevel='4', orientation='column')
                {% endset %}
                {{log("Adding missing partition in range from " ~ val[0] ~ " till "~ val[1] ~ " for table " ~ tgt_tab,true)}}
                {% do run_query(query) %}
            {% endfor %}
            {% if results|length > 0 %}
                {{log("Adding missing partition done!",true)}}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro truncate_partition_raw(src_clause, tgt_tab, partition_type, partition_column_type) %}
{# функция анализиирует табличных имен список партиций в src_tab (батч) по полю партиционирования в tgt_tab и выполняет транкейт по ним в tgt_tab
    Параметры: 
        src_clause - выражение для получения списка значений партиционирования
        tgt_tab - конечная таблица (витрина)
        partition_type - тип партиционирования (range/list)
        partition_column_type - тип поля партиционирования
#}
    {% if partition_type is none or partition_column_type is none %}
        {{ log("no partitioning data for  " ~ tgt_tab ~ ", exiting", true) }}
        {{ return("") }}
    {% endif %}
    {% if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] and load_relation(tgt_tab) is not none %}
        {% set partitions = get_partition_names(src_clause, tgt_tab, partition_type, partition_column_type) %}
        {% for table in partitions %}
            {% set partition = table["partition_name"] %}
            {% set val_start = table["range_start"] %}
            {% set val_end = table["range_end"] %}
            {{log("truncating partition " ~ partition ~ " for values from " ~ val_start ~ " to " ~ val_end,true)}}
            {% set query %}
                truncate table {{partition}}
            {% endset %}
            {% do run_query(query) %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro gather_partition_stats_raw(src_clause, tgt_tab, partition_type, partition_column_type) %}
{# функция анализиирует список табличных имен партиций в src_tab (батч) по полю партиционирования в tgt_tab и собирает статистику по ним в tgt_tab
    Параметры: 
        src_clause - исходная таблица для анализа (батч)
        tgt_tab - конечная таблица (витрина)
        partition_type - тип партиционирования (range/list)
        partition_column_type - тип поля партиционирования
#}
    {% if partition_type is none or partition_column_type is none %}
        {{ log("no partitioning data for  " ~ tgt_tab ~ ", exiting", true) }}
        {{ return("") }}
    {% endif %}
    {%- if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] and load_relation(tgt_tab) is not none -%}
        {% set partitions = get_partition_names(src_clause, tgt_tab, partition_type, partition_column_type) %}
        {% for table in partitions %}
            {% set partition = table["partition_name"] %}
            {% set val_start = table["range_start"] %}
            {% set val_end = table["range_end"] %}
            {{log("analyzing partition " ~ partition ~ " for values from " ~ val_start ~ " to " ~ val_end,true)}}
            {% set query  %}
                analyze {{partition}}
            {% endset %}
            {% do run_query(query) %}
        {% endfor %}
    {% endif %}
{% endmacro %}