WITH LaggedProducts AS (
  SELECT event_id, event_type, event_timestamp, shopper_id, payload.transaction_id,
  unnest(LAG(payload.products, 1, '[]') OVER (PARTITION BY shopper_id, payload.transaction_id ORDER BY event_timestamp)) AS previous_products
  FROM {{ ref('src_events') }}
  WHERE event_type = 'cartUpdated'
)

,CurrentProducts AS (
  SELECT event_id, event_type, event_timestamp, shopper_id, payload.transaction_id,
  payload.transaction_id,
  unnest(payload.products) as current_product
  FROM {{ ref('src_events') }}
  WHERE event_type = 'cartUpdated'
)

,LaggedEvents AS (
	select 
		event_id, event_type, event_timestamp, shopper_id, transaction_id,
		unnest(previous_products) as previous_products
	from LaggedProducts
)

,CurrentEvents AS (
	select 
		event_id, event_type,event_timestamp, shopper_id, transaction_id,
		unnest(current_product) as current_product
	from CurrentProducts
)

select
	COALESCE(ce.event_type,le.event_type) as event_type, 
	COALESCE(ce.event_id,le.event_id) as event_id,
	COALESCE(ce.product_id,le.product_id) as product_id,
    COALESCE(ce.shopper_id,le.shopper_id) as shopper_id,
    COALESCE(ce.transaction_id,le.transaction_id) as transaction_id,
	COALESCE(ce.quantity, 0) - COALESCE(le.quantity, 0) as quantity_added,
	COALESCE(ce.event_timestamp, le.event_timestamp) as event_timestamp
from CurrentEvents ce
full outer join LaggedEvents le
	on ce.event_id = le.event_id
	and ce.product_id = le.product_id
order by transaction_id, event_timestamp