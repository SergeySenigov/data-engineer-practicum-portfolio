DROP TABLE if exists dds.fct_courier_deliveries;

CREATE TABLE dds.fct_courier_deliveries (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	delivery_id int4 NOT NULL,
	rate int2 NOT NULL,
	delivery_sum numeric(14, 2) NOT NULL default 0,
	tip_sum numeric(14, 2) NOT NULL default 0,
	CONSTRAINT fct_courier_deliveries_pk PRIMARY KEY (id),
	CONSTRAINT fct_courier_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
	CONSTRAINT fct_courier_deliveries_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id)
);