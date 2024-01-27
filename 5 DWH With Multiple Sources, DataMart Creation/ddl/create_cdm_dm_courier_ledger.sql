drop table if exists cdm.dm_courier_ledger ;

create table cdm.dm_courier_ledger(
	id serial,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year	int NOT null ,
	settlement_month int NOT null check (settlement_month between 1 and 12),
	orders_count int NOT null default 0 check (orders_count >=0),
	orders_total_sum numeric(14, 2) NOT null default 0 check (orders_total_sum >=0),
	rate_avg float NOT NULL check (rate_avg between 1 and 5),
	order_processing_fee numeric NOT NULL default 0 check (orders_count >=0) , 
	courier_order_sum numeric(14, 2) NOT NULL default 0 check (courier_order_sum >=0),
	courier_tips_sum numeric(14, 2) NOT NULL default 0 check (courier_tips_sum >=0),
	courier_reward_sum numeric(14, 2) NOT NULL default 0 check (courier_reward_sum >=0),
	
	CONSTRAINT dm_courier_ledger_pk PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_courier_settlement_year_month_unique unique (courier_id, settlement_year, settlement_month)
);