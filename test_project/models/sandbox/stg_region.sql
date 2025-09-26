{{ config(
	tags=['stg_region', 'region'],
	distributed_by='region_pk'
)}}

{%- set yaml_metadata -%}
source_model: raw_region
derived_columns:
  record_source: '''manzana''::varchar'
  process_code: '''{{ var("process_code") }}''::date'
  load_datetime: now()
hashed_columns:
  region_pk:
  - region_id
  - record_source
  region_hashdiff:
    is_hashdiff: true
    columns:
    - region_guid
    - region_name
    - external_id
    - type
    - deleted_flg
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ dbtvault.stage(
	include_source_columns=true,
	source_model=metadata_dict['source_model'],
	derived_columns=metadata_dict['derived_columns'],
	hashed_columns = metadata_dict['hashed_columns']
)}}