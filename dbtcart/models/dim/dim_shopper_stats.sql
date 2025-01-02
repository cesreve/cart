with base as (
select 
	shopper_id, event_id, event_type, event_timestamp, transaction_id, transaction_status,  unnest(products, recursive := true) as products
from {{ ref('fct_events') }}
where transaction_status='Completed')


,cancel_carts as  (
select shopper_id, count(event_id) as canceled_carts
from {{ ref('fct_events') }}
where event_type='cartCanceled'
group by shopper_id)

,agg_kpis as (
select 
	shopper_id
	,sum(total_price) as total_spent
	,sum(quantity) as total_articles
	,count(distinct transaction_id) as total_completed_transactions
	,max(event_timestamp) as last_cart_date
from base
group by shopper_id)

select
	a.shopper_id, a.total_spent, a.total_articles, a.total_completed_transactions, a.last_cart_date
	,COALESCE(c.canceled_carts, 0) as canceled_carts
from agg_kpis a
left join cancel_carts c
	on a.shopper_id = c.shopper_id
