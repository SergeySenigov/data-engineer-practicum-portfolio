drop table if exists public.shipping_status ;

create table public.shipping_status ( 
 shippingid int8,
 status text NOT NULL,
 state text NOT NULL,
 shipping_start_fact_datetime timestamp NOT NULL,
 shipping_end_fact_datetime timestamp NULL);

 alter table public.shipping_status add constraint shipping_status_p_key primary key (shippingid) ;

 alter table public.shipping_status add constraint check_status check (status in ('in_progress', 'finished'));
 alter table public.shipping_status add constraint check_dates check (shipping_start_fact_datetime <= shipping_end_fact_datetime);

insert into public.shipping_status(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with ts as (select s1.shippingid, 
	first_value(s1.status) over (partition by shippingid order by state_datetime desc ) last_status,
	first_value(s1.state) over (partition by shippingid order by state_datetime desc ) last_state,
	max(case when s1.state = 'booked' then s1.state_datetime end) over (partition by shippingid) start_dt,
	max(case when s1.state = 'recieved' then s1.state_datetime end) over (partition by shippingid) end_dt
	--row_number() over (partition by shippingid order by state_datetime desc ) rown
	from shipping s1)
select distinct s.shippingid, ts.last_status, ts.last_state, ts.start_dt, ts.end_dt--, ts.rown
from shipping s
left join ts on s.shippingid = ts.shippingid --and ts.rown = 1
;

select * from public.shipping_status
limit 5;