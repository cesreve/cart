select event_id, shopper_id, event_type , event_timestamp,
	unnest(payload) 
from {{ ref('src_events') }}
