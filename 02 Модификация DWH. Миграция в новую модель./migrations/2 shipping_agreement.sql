drop table if exists public.shipping_agreement cascade;

create table public.shipping_agreement ( 
 agreementid int4 not null,
 agreement_number text not null,
 agreement_rate numeric(14,4) not null,
 agreement_commission numeric(14,4) not null,
 constraint shipping_agreement_p_key primary key  (agreementid));
 
alter table public.shipping_agreement add constraint check_agreement_rate check (agreement_rate > 0 and agreement_rate < 1);
alter table public.shipping_agreement add constraint check_agreement_commission check (agreement_commission > 0 and agreement_commission < 1);

insert into public.shipping_agreement(agreementid, agreement_number, agreement_rate, agreement_commission)
select distinct 
(regexp_split_to_array(s.vendor_agreement_description, E'\\:'))[1]::int ,
(regexp_split_to_array(s.vendor_agreement_description, E'\\:'))[2]::text ,
CAST ( (regexp_split_to_array(s.vendor_agreement_description, E'\\:'))[3] AS numeric) ,
CAST ( (regexp_split_to_array(s.vendor_agreement_description, E'\\:'))[4] AS DOUBLE PRECISION)
from public.shipping s ;

select * from public.shipping_agreement
limit 5;
