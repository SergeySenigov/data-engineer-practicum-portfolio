drop view if exists analysis.orders ;

create view analysis.orders as 
with t as (
  select osl.order_id, osl.status_id,
  	last_value(osl.status_id) 
  			over (partition by osl.order_id
				  order by osl.dttm asc
				 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) log_last_status_id
  from production.orderstatuslog osl),
  t2 as (select t.order_id, min(t.log_last_status_id) log_last_status_id
		 from t 
		 where t.status_id = t.log_last_status_id
		 group by t.order_id)
select o.order_id, o.order_ts, t2.log_last_status_id as status, o.user_id, o.bonus_payment, o.payment, o."cost", o.bonus_grant
from production.orders o
left join t2 on o.order_id = t2.order_id
