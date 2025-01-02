{{ config(store_failures = true) }}

with step1 as (
select
	event_id, 
	payload.total_transaction_amount,
	payload.transaction_id,
	unnest(payload.products, recursive := true) as products
from {{ ref('src_events') }} )

, calc as (
select 
	event_id, transaction_id , sum(total_price) as sum_total_price, max(total_transaction_amount) as total_transaction_amount
from step1
group by transaction_id, event_id
order by event_id
)

select * 
from calc
where sum_total_price <> total_transaction_amount