drop table if exists public.shipping_info ;

create table public.shipping_info ( 
 shippingid int8,
 vendorid bigint NULL,
 payment_amount numeric(14,2) NULL,
 shipping_plan_datetime  timestamp NULL,
 transfer_type_id int8 not null,
 shipping_country_id int8 not null,
 agreementid int8 not null,
 constraint shipping_info_p_key primary key (shippingid));

 alter table public.shipping_info add constraint shipping_info_transfer_type_id_fkey foreign key (transfer_type_id) references shipping_transfer(id);

 alter table public.shipping_info add constraint shipping_info_shipping_country_id_fkey foreign key (shipping_country_id) references shipping_country_rates(id);

 alter table public.shipping_info add constraint shipping_agreementid_fkey foreign key (agreementid) references shipping_agreement(agreementid);

 alter table public.shipping_info add constraint check_payment_amount check (payment_amount > 0);
 
insert into public.shipping_info(shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
with st as (select id as transfer_type_id, transfer_type, transfer_model
  from shipping_transfer),
  scr as (select id as shipping_country_id, shipping_country 
		 from shipping_country_rates t)
select 
	distinct s.shippingid, s.vendorid, s.payment_amount, s.shipping_plan_datetime,
	st.transfer_type_id, scr.shipping_country_id, 
	(regexp_split_to_array(s.vendor_agreement_description, E'\\:'))[1]::int
from shipping s
left join st on (regexp_split_to_array(s.shipping_transfer_description, E'\\:'))[1]::text = st.transfer_type 
	and (regexp_split_to_array(s.shipping_transfer_description, E'\\:'))[2]::text = st.transfer_model
left join scr on s.shipping_country = scr.shipping_country;

select * from public.shipping_info
limit 5;