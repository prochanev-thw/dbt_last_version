{{ config(
 tags=['dm_region', 'loyalty', 'dm'],
 distributed_by='region_pk'
) }}


{% set region =
    snapshot(src_tab = ref('sat_region'),
            src_hub = ref('hub_region'),
            src_pk = 'region_pk',
            src_nk = 'region_id',
            src_eff = 'version_id',
            src_cols = ['region_guid',
                    'region_name',
                    'external_id',
					'type',
                    'record_source',
                    'dt_load',
					'deleted_flg',
                    'version_id',],
            clause = 'deleted_flg = 0')
%}


SELECT
    sc.region_pk,
	sc.region_id,
	sc.version_id as last_version,
	'I' as sys_change_operation,
	sc.region_guid,
	sc.region_name,
	sc.external_id,
	sc.type,
	sc.dt_load,
	sc.record_source
FROM ({{region}}) sc
