{{ config(
	tags=['sat_region', 'region'],
	distributed_by='region_pk'
)}}

{%- set yaml_metadata -%}
source_model: stg_region
src_pk: region_pk
src_hashdiff:
  source_column: region_hashdiff
  alias: hashdiff
src_payload:
- region_guid
- region_name
- external_id
- type
- deleted_flg
src_ldts: load_datetime
src_source: record_source
src_eff: version_id
src_extra_columns:
- modified_on
- dt_load
- process_code
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ dbtvault.postgres__sat_ext(
	src_pk=metadata_dict['src_pk'],
	src_hashdiff=metadata_dict['src_hashdiff'],
	src_eff=metadata_dict['src_eff'],
	src_ldts=metadata_dict['src_ldts'],
	src_source=metadata_dict['src_source'],
	src_payload=metadata_dict['src_payload'],
	source_model=metadata_dict['source_model'],
	src_extra_columns=metadata_dict['src_extra_columns']
)}}