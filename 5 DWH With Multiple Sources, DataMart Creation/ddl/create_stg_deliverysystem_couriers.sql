DROP TABLE if exists stg.deliverysystem_couriers;

CREATE TABLE stg.deliverysystem_couriers (
	id serial4 NOT null ,
	courier_id varchar not null, 
	"name" varchar NOT null,
	
	CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT deliverysystem_couriers_courier_id_uindex UNIQUE (courier_id)
);
