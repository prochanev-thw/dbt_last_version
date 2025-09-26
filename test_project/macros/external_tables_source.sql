{% macro get_meta_fields_string(process_date) %}
{# получаем строку fields_string (в формате colname datatype,) из метаданных модели.
  корретируем поля по наличию атрибута load_since относительно указанной в process_date даты #}
  {% set collist = [] %}
  {% for column, column_data in model.columns.items() %}
    {% if not column_data.load_since or process_date is not defined %}
      {{ collist.append(column + " " + column_data.data_type) }}
    {% elif string_to_date(column_data.load_since) <= process_date %}
      {{ collist.append(column + " " + column_data.data_type) }}
    {% endif %}
  {% endfor %}
  {{ return(collist|join(",\n")) }}
{% endmacro %}

{% macro get_query_string(process_date) %}
{# получаем список колонок (в формате colname,) из метаданных модели.
  заменяем поле на выражение null::data_type as colname, в случае, если атрибут load_since
  меньше даты process_date #}
  {% set collist = [] %}
  {% for column, column_data in model.columns.items() %}
    {% if not column_data.load_since or string_to_date(column_data.load_since) <= process_date%}
      {{ collist.append(column) }}
    {% else %}
      {{ collist.append("null::" + column_data.data_type + " as " + column) }}
    {% endif %}
  {% endfor %}
  {{ return(collist|join(",\n")) }}
{% endmacro %}

{% macro get_file_type(config, default=none) %}
  {%- set file_type_from_config = config.get('s3_file_type') %}
    {{log("file_type_from_config: " ~ file_type_from_config ,true)}}
  {%- if file_type_from_config is not none %}
    {{ return(file_type_from_config) }}
  {%- elif path is not none %}
    {{ return(config.s3_key_pattern.split(".")|last) }}
  {%- else %}
    {{ return(default) }}
  {%- endif %}
{% endmacro %}

{% macro generate_location(config, process_date) %}
{# генерация строки location для внешней таблицы по атрибутам из словаря config и даты process_date #}
  {% set bucket = config.s3_bucket %}
  {% set path = config.s3_key_pattern %}
  {% set date_format = config.s3_date_format %}
  {% set increment = modules.datetime.datetime.strftime(process_date,date_format) %}
  {% set fullpath = bucket + "/" + path|replace("[logical_date]", increment) %}
  {% if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation"] %}
    {{ log("Loading data from " ~ fullpath, true) }}
  {% endif %}
  {%- set file_type = get_file_type(config=config, default='parquet') %}
  {% set profile = config.s3_profile %}
  {% set endpoint = "storage.yandexcloud.net" %}
  {% set location = "pxf://" + fullpath + "?PROFILE=s3:" + file_type + "&SERVER="+ profile +"&endpoint=" + endpoint%}
  {{ return(location) }}
{% endmacro %}

{% macro exttable(config, process_code) %}
{# макрос создания внешней таблицы greenplum и выборки ее содержимого в запросе #}
  {% set tabname = this.table + "_external" %}
  {% set dt = string_to_date(process_code) %}
  {% if config.s3_load_type == "T-1" %}
    {% set dt = dt - modules.datetime.timedelta(1) %}
  {% endif %}
  {% set location = generate_location(config, dt) %}
  {%- set query -%}
    create external temporary table {{tabname}} (
      {{get_meta_fields_string(dt)}}
    )
    location ( '{{location}}' ) on all
    format 'CUSTOM' ( formatter='pxfwritable_import' )
    encoding 'UTF8'
  {%- endset -%}
  {% if execute and flags.WHICH in ["run", "test", "seed", "snapshot", "run-operation", "build"] %}
    {% do run_query(query) %}
  {% endif %}
  select {{ get_query_string(dt)}}
  from {{ tabname }}
{% endmacro %}