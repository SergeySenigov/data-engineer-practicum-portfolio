create or replace view public.shipping_datamart as (
 select si.shippingid, si.vendorid, si.transfer_type_id,
 (DATE_PART('day', ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime))::int as full_day_at_shipping,
 coalesce((ss.shipping_end_fact_datetime > si.shipping_plan_datetime), false) as is_delay,
 (ss.status = 'finished') as is_shipping_finish,
 (case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 
	DATE_PART('day', ss.shipping_end_fact_datetime - si.shipping_plan_datetime)
	end)::int as delay_day_at_shipping,
 si.payment_amount,
 si.payment_amount*(cr.shipping_country_base_rate+sa.agreement_rate+st.shipping_transfer_rate) as vat,
 si.payment_amount*agreement_commission as profit
 from public.shipping_info si
 left join public.shipping_status ss on ss.shippingid = si.shippingid
 left join public.shipping_agreement sa on sa.agreementid = si.agreementid
 left join public.shipping_country_rates cr on cr.id = si.shipping_country_id
 left join public.shipping_transfer st on st.id = si.transfer_type_id);