drop table if exists public.shipping_country_rates cascade;

create table public.shipping_country_rates ( 
 id serial,
 shipping_country text not null,
 shipping_country_base_rate numeric(14,3) not null,
 constraint   shipping_country_rates_p_key primary key  (id));
---
alter table public.shipping_country_rates add constraint shipping_country_unique UNIQUE  (shipping_country);
alter table public.shipping_country_rates add constraint check_sh_cont_base_rate check (shipping_country_base_rate > 0 and shipping_country_base_rate < 1);

insert into public.shipping_country_rates(shipping_country, shipping_country_base_rate) 
select distinct s.shipping_country, s.shipping_country_base_rate
from shipping s ;

select * from public.shipping_country_rates
limit 5;
