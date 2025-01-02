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



/*
SELECT 
    shopper_id,
    COUNT (*) AS total_transactions,
    COUNT(CASE WHEN event_type = 'cartValidated' THEN 1 END) AS validated_carts,
    COUNT(CASE WHEN event_type = 'cartCanceled' THEN 1 END) AS cancelled_carts,
    CAST(COUNT(CASE WHEN event_type = 'cartValidated' THEN 1 END) AS REAL) * 100 / COUNT(*) AS conversion_rate,
    SUM(CASE WHEN event_type = 'cartValidated' THEN total_price ELSE 0 END) AS total_spent,
    AVG(CASE WHEN event_type = 'cartValidated' THEN total_price END) AS average_cart_value,
    MAX(event_timestamp) AS last_cart_date,
    (SELECT device FROM fct_events WHERE shopper_id = e.shopper_id GROUP BY device ORDER BY COUNT(*) DESC LIMIT 1) AS preferred_device,
    (SELECT payment_method FROM fct_events WHERE shopper_id = e.shopper_id GROUP BY payment_method ORDER BY COUNT(*) DESC LIMIT 1) AS preferred_payment_method
FROM fct_events e
GROUP BY shopper_id
*/