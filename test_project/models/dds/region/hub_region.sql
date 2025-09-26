{{ config(
	tags=['hub_region', 'region'],
	distributed_by='region_pk'
)}}

{%- set yaml_metadata -%}
source_model:
- stg_region
src_pk: region_pk
src_nk:
- region_id
src_ldts: load_datetime
src_source: record_source
src_extra_columns:
- process_code
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ dbtvault.hub(
	src_pk=metadata_dict['src_pk'],
	src_nk=metadata_dict['src_nk'],
	src_ldts=metadata_dict['src_ldts'],
	src_source=metadata_dict['src_source'],
	source_model=var('source_hub_override', default=metadata_dict['source_model']),
	src_extra_columns=metadata_dict['src_extra_columns']
)}}