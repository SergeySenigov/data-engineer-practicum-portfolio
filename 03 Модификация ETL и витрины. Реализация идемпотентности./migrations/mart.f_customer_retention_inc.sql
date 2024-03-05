delete from mart.f_customer_retention 
where f_customer_retention.period_id =
   (select substr(d_calendar.week_of_year_iso, 1, 8) from mart.d_calendar where d_calendar.date_actual = '{{ds}}' ) ;

insert into mart.f_customer_retention 
   (period_id, period_name, item_id,
    new_customers_count, new_customers_revenue,
	returning_customers_count, returning_customers_revenue,
	refunded_customer_count, customers_refunded)
select week_of_year, 'weekly' period_name, item_id,
    count (case when shipped_count = 1 then 1 end) cust_ord_cnt_eq_1,
	sum(case when shipped_count = 1 then shipped_pmt else 0 end) cust_pmt_ord_cnt_eq_1,
	count (case when shipped_count > 1 then 1 end) cust_ord_cnt_more_1,
	sum(case when shipped_count >1 then shipped_pmt else 0 end) cust_pmt_ord_cnt_more_1,
	count (case when refunded_count >0 then 1 end) cust_cnt_refund,
	sum(case when refunded_count >0 then refunded_qty else 0 end) cust_qty_refund
from (
select item_id as item_id, substr(d_calendar.week_of_year_iso, 1, 8) week_of_year ,
uol2.customer_id, count(case when status = 'shipped' then 1 end) 
	shipped_count,
	sum(case when status = 'shipped' then uol2.payment_amount end) shipped_pmt,
	count(case when status = 'refunded' then 1 end) refunded_count,
	sum(case when status = 'refunded' then uol2.quantity  end) refunded_qty
	from staging.user_order_log uol2 
	join mart.d_calendar on uol2.date_time::date = d_calendar.date_actual 
			and '{{ ds }}' between d_calendar.first_day_of_week and d_calendar.last_day_of_week
	
	group by item_id, substr(d_calendar.week_of_year_iso, 1, 8) , customer_id
) t
group by item_id, week_of_year;