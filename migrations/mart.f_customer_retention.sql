insert into mart.f_customer_retention 
(new_customers_count, returning_customers_count, refunded_customer_count, period_name, 
period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
select
 count(distinct case when order_count = 1 then customer_id end) as new_customers_count,
 count(distinct case when order_count > 1 then customer_id end) as returning_customers_count,
 count(distinct case when refund_count > 0 then customer_id end) as refunded_customer_count,
 'weekly' as period_name,
 extract(week from max(last_order_date)) as period_id,
 item_id,
 sum(distinct case when order_count = 1 then payment_sum end) as new_customers_revenue,
 sum(distinct case when refund_count > 0 then payment_sum end) as returning_customers_revenue,
 sum(refund_count) as customers_refunded 
from (
select 
count(uol.uniq_id) as order_count,
uol.item_id,
uol.customer_id,
sum(fs.payment_amount) as payment_sum, 
count(distinct case when uol.status = 'refunded' then uol.uniq_id end) as refund_count, 
max(uol.date_time) as last_order_date
from staging.user_order_log uol inner join mart.f_sales fs on uol.customer_id = fs.customer_id and uol.item_id=fs.item_id 
group by uol.item_id, uol.customer_id
) as t1
group by item_id
having max(last_order_date)::Date = '{{ds}}';