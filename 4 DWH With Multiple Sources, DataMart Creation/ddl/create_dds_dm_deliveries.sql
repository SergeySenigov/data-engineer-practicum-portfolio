DROP TABLE if exists dds.dm_deliveries;

CREATE TABLE dds.dm_deliveries(
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL UNIQUE,
	delivery_ts timestamp NOT NULL,
	order_id int4 NOT NULL,
	address varchar,
	CONSTRAINT dm_deliveries_pk PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
);