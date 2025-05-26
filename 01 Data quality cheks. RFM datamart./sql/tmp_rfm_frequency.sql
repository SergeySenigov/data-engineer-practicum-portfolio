delete from analysis.tmp_rfm_frequency ;

with t as (select u.id as user_id,
	max(case when oss."key" = 'Closed' then order_ts end) max_dttm
	, sum(case when oss."key" = 'Closed' then o.payment end) order_payment
	, sum(case when oss."key" = 'Closed' then 1 else 0 end) order_count
from analysis.users u 
left join analysis.orders o on u.id = o.user_id
left join analysis.orderstatuses oss on oss.id = o.status
group by u.id),
t2 as (select user_id, order_count, ntile(5) over (order by order_count asc) tile
			from t)
insert into analysis.tmp_rfm_frequency(
select user_id, tile
from t2
)