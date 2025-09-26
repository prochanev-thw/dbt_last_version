
{% macro incremental_insert(tmp_relation, target_relation, unique_key=none) %}
    {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {%- if unique_key is not none -%}
    delete
    from {{ tmp_relation }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ target_relation }}
    );
    {%- endif -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}


{% macro reload_report_date(tmp_relation, target_relation, column_name=none,report_date=none) %}
    {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    delete
    from {{ target_relation }}
    where {{ column_name }} = '{{ report_date }}'::date;
    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}


{% macro drop_temporary_table(tmp_relation) %}
  {% call statement('drop_temp_table', fetch_result=False) -%}
    DROP TABLE {{ tmp_relation }};
  {%- endcall %}
{%- endmacro %}

{% macro get_prev_date() %}
    {% call statement('prev_date', fetch_result=True) -%}
      select (now()::date - interval '1 day')::date as prev_date;
    {%- endcall %}
    {{ return(load_result('prev_date').table[0]['prev_date']) }}
{%- endmacro %}

{% macro get_last_date(target_relation,over_column,start_from_new_date) %}
    {% if start_from_new_date %}
      {% call statement('last_date', fetch_result=True) -%}
        select max({{ over_column }})::date + interval '1 day' as last_date from {{ target_relation }};
      {%- endcall %}
    {% else %}
      {% call statement('last_date', fetch_result=True) -%}
        select max({{ over_column }})::date as last_date from {{ target_relation }};
      {%- endcall %}
    {% endif %}
    {{ return(load_result('last_date').table[0]['last_date']) }}
{%- endmacro %}

{% macro get_list_days(from_date, to_date) %}
    {% set list_days = [] %}
    {% call statement('list_days', fetch_result=True) -%}
        SELECT
            d::date as day
        FROM
            generate_series('{{from_date}}'::date, '{{to_date}}'::date, '1 day'::interval) d ;   
    {%- endcall %}

    {% for day in load_result('list_days')['data']%}
        {{ list_days.append(day[0]|string) }}
    {% endfor %}
    {{ return(list_days) }}
{%- endmacro %}

{% materialization incremental_over_date, default -%}
  {% set unique_key = config.get('unique_key') %}
  {% set over_column = config.require('over_column') %}
  {% set start_date = config.get('start_date') %}
  {% set stop_date = config.get('stop_date') %}
  {% set list_date = config.get('list_date', default=[]) %}
  {% set period = config.get('period', default='day') %}
  {% set start_from_new_date = config.get('start_from_new_date', default=True) %}
  {% set incremental_strategy = config.get('incremental_strategy',default='incremental_insert') %}
  {% set full_refresh_mode = flags.FULL_REFRESH %}
  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% if list_date| length == 0 %}  
    {% if start_date is none and existing_relation is none %}
      {% set start_date = get_prev_date() %}
    {% elif  start_date is none and existing_relation is not none %}
        {% set start_date = get_last_date(target_relation,over_column,start_from_new_date) %}
        {% if start_date is none %} 
          {% set start_date = get_prev_date() %}
        {% endif %}    
    {% endif %}
		{% if stop_date is none %}
        {% set stop_date = get_prev_date() %}
    {% endif %}
		{% set list_date = get_list_days(start_date, stop_date) %}
  {% endif %}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  
  -- Dirty hack
  {% call statement('main', fetch_result=True) -%}
      COMMIT;
  {%- endcall %}

  {%- set loop_vars = {'sum_rows_inserted': 0, 'rows_inserted': 0} -%}

  {{ log(list_date, True) }}
  -- commit each date as a separate transaction
  {% for column_date in list_date %}
    {%- set period = loop.index -%}
    {%- set msg = "Running for date " ~ column_date -%}
    {{ log(msg, True) }}
  	-- `BEGIN` happens here:
  	{{ run_hooks(pre_hooks, inside_transaction=True) }}

  	{% set to_drop = [] %}
    {% set build_sql = '' %}
    {{ log(existing_relation, True) }}
  	{% if existing_relation is none %}
      	{% set build_sql = create_table_as(False, target_relation, sql) | replace('_COLUMN_DATE_', "'" ~ column_date ~ "'" ~ "::date") %}
  	{% elif existing_relation.is_view or full_refresh_mode %}
      	{#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      	{% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      	{% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      	{% do adapter.drop_relation(backup_relation) %}

      	{% do adapter.rename_relation(target_relation, backup_relation) %}
      	{% set build_sql = create_table_as(False, target_relation, sql) | replace('_COLUMN_DATE_', "'" ~ column_date ~ "'" ~ "::date") %}
      	{% do to_drop.append(backup_relation) %}
  	{% else %}
      	{% set tmp_relation = make_temp_relation(target_relation) %}
        {% set build_sql = create_table_as(True, tmp_relation, sql) | replace('_COLUMN_DATE_', "'" ~ column_date ~ "'" ~ "::date") %}
      	{% do run_query(build_sql) %}
      	{#-- do adapter.expand_target_column_types(
          --   	from_relation=tmp_relation,
          --   	to_relation=target_relation) #}
        {% if incremental_strategy == 'incremental_insert' %}
      	  {% set build_sql = incremental_insert(tmp_relation, target_relation, unique_key=unique_key) %}
        {% elif incremental_strategy == 'reload_report_date' %}
          {% set build_sql = reload_report_date(tmp_relation, target_relation, column_name=over_column, report_date=column_date) %}
        {% else %}
          {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
        {% endif %}
  	{% endif %}

    {%- set name = 'main-' ~ column_date -%}
  	{% call statement(name,fetch_result=True) %}
      	{{ build_sql }}
  	{% endcall %}
    
  	{{ run_hooks(post_hooks, inside_transaction=True) }}
  	-- `COMMIT` happens here
  	{% do adapter.commit() %}

    -- Load 
    
    

    {% set rows_inserted = 0 %}
    {% if 'INSERT' in load_result(name)['status']%}
      {%- set rows_inserted = (load_result(name)['status'].split(" "))[2] | int -%}
    {% else%}
      {%- set rows_inserted = (load_result(name)['status'].split(" "))[1] | int -%}
    {% endif %}
    
    {%- set sum_rows_inserted = loop_vars['sum_rows_inserted'] + rows_inserted -%}
    {%- if loop_vars.update({'sum_rows_inserted': sum_rows_inserted}) %} {% endif -%}

    {%- set msg = "Ran for " ~ column_date ~ " date; " ~ rows_inserted ~ " records inserted; " ~ sum_rows_inserted ~ " sum records inserted;" -%}
    {{ log(msg, True) }}

    {% if existing_relation is none %}
      {{ return({'relations': [target_relation]}) }}
    {% endif%}

    {{ drop_temporary_table(tmp_relation) }}
  {% endfor %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
