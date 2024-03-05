drop table if exists public.shipping_transfer cascade ;

create table public.shipping_transfer ( 
 id serial,
 transfer_type varchar(5) not null,
 transfer_model varchar(20) not null,
 shipping_transfer_rate numeric(14,3) not null,
 constraint shipping_transfer_p_key primary key  (id));

alter table public.shipping_transfer add constraint check_shipping_transfer_rate check (shipping_transfer_rate > 0 and shipping_transfer_rate < 1);

insert into public.shipping_transfer(transfer_type, transfer_model, shipping_transfer_rate)
select distinct 
(regexp_split_to_array(s.shipping_transfer_description, E'\\:'))[1]::text ,
(regexp_split_to_array(s.shipping_transfer_description, E'\\:'))[2]::text ,
CAST ( s.shipping_transfer_rate AS DOUBLE PRECISION) 
from public.shipping s;

select * from public.shipping_transfer
limit 5;