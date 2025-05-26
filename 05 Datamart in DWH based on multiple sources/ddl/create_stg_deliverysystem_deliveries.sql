DROP TABLE if exists stg.deliverysystem_deliveries;

CREATE TABLE stg.deliverysystem_deliveries (
	id serial4 NOT null ,
	order_id varchar not null, 
	order_ts timestamp not null,
	delivery_id varchar not null,
	courier_id varchar not null,
	address varchar not null,
	delivery_ts varchar not null,
	rate int not null check (rate>=1 and rate <=5),
	delivery_sum numeric(14,2) not null default 0,
	tip_sum numeric(14,2) not null default 0,
	
	CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT deliverysystem_deliveries_delivery_id_uindex UNIQUE (delivery_id)
);