select event_id, shopper_id, event_type , event_timestamp,
	unnest(payload) 
from {{ ref('src_events') }}
 
 
 
 
 
 
 
 /*
 with step1 as (
	select
		event_id 
		,shopper_id 
		,event_type 
		,event_timestamp 
		,unnest(payload, recursive := true)
	from {{ ref('src_events') }}
)

,products as (
	select 
		event_id
		,unnest(products, recursive := true) as products
	from step1
)

select 
	step1.* exclude(products)
	,p.* exclude(event_id)
from step1
left join products p
	on step1.event_id = p.event_id
order by event_timestamp
*/