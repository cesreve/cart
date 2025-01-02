with step1 as (
SELECT 
    event_id,
    date_trunc('day', event_timestamp) as event_date,
    shopper_id, 
    event_type, 
    transaction_status,
    total_transaction_amount,
    products,
    RANK() OVER (PARTITION BY transaction_id ORDER BY event_timestamp DESC) AS transaction_step
FROM {{ ref('fct_events') }}
)

,step2 as (
select *
from step1
where 1=1
	and transaction_step = 1)

,kpis1 as (
select event_date, sum(quantity) as articles_qty
from (
	select 
		event_date 
		,unnest(products, RECURSIVE := true) 
	from step2
	)
group by event_date
)

,kpis2 as (
select     
	event_date,
	COUNT(DISTINCT shopper_id) AS active_shoppers,
	SUM(total_transaction_amount) AS revenue,
	COUNT(CASE WHEN event_type = 'cartValidated' THEN 1 END) AS validated_carts,
	COUNT(CASE WHEN event_type = 'cartCanceled' THEN 1 END) AS canceled_carts
from step2
group by event_date)

select
	COALESCE(k1.event_date, k2.event_date) as event_date
	,COALESCE(k1.articles_qty, 0) as articles_qty
	,COALESCE(k2.active_shoppers, 0) as active_shoppers
	,COALESCE(k2.revenue, 0) as revenue
	,COALESCE(k2.validated_carts, 0) as validated_carts
	,COALESCE(k2.canceled_carts, 0) as canceled_carts
from kpis1 k1
full outer join kpis2 k2
	on k1.event_date= k2.event_date