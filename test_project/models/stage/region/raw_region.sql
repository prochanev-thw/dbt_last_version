{{ config(
	tags=['raw_region', 'region'],
	materialized='view'
)}}

SELECT last_version as version_id,
	case when (sys_change_operation) = 'D' then 1 else 0 end as deleted_flg,
	regionId as region_id,
	regionGuid as region_guid,
	regionname as region_name,
	externalid as external_id,
	Type as type,
	to_timestamp(ModifiedOn)::timestamp at time zone 'MSK' at time zone 'UTC' as modified_on,
	dt_load at time zone 'MSK' at time zone 'UTC' as dt_load
FROM {{source('manzana', 'loyalty__crmdata__region')}}
WHERE instance_id = 3