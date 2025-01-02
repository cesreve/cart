select 	event_id, shopper_id, event_type, event_timestamp, transaction_id, transaction_status
from (
	select
		event_id, shopper_id, event_type, event_timestamp,
        transaction_id,
		transaction_status,
		rank() OVER (PARTITION BY transaction_id ORDER BY event_timestamp DESC) as rk 
	from {{ ref('fct_events') }}
	)
where 1=1
	and rk = 1