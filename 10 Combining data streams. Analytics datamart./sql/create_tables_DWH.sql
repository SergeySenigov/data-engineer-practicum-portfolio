drop table if exists STV2023060652__DWH.l_currency_operation;
drop table if exists STV2023060652__DWH.l_currency_operation_date;
drop table if exists STV2023060652__DWH.s_currency_rates;
drop table if exists STV2023060652__DWH.h_currencies;
drop table if exists STV2023060652__DWH.h_operations;

create table STV2023060652__DWH.h_operations
(
    hk_operation_id int not null,
    operation_id uuid not null,
    load_dt datetime not null,
    load_src varchar(60),
    constraint c_h_operation_pk primary key (hk_operation_id) ENABLED
)
order by load_dt
SEGMENTED BY hk_operation_id all nodes
PARTITION BY load_dt::date ;

create table STV2023060652__DWH.h_currencies
(
    hk_currency_id int not null,
    currency_code varchar(5) not null,
    load_dt datetime not null,
    load_src varchar(60),
    constraint c_h_сurrencies_pk primary key (hk_currency_id) ENABLED
)
order by load_dt
SEGMENTED BY hk_currency_id all nodes
PARTITION BY load_dt::date ;

create table STV2023060652__DWH.s_currency_rates
(
    hk_currency_id int CONSTRAINT fk_s_currency_rates_h_currencies REFERENCES STV2023060652__DWH.h_currencies(hk_currency_id),
    date_update date not null,
    rate numeric(12,5) not null CONSTRAINT c_s_currency_rates_rate_positive check(rate>0),
    load_dt datetime not null,
    load_src varchar(60),
    hk_currency_rates_hashdiff int not null
)
order by load_dt
SEGMENTED BY hk_currency_id all nodes
PARTITION BY load_dt::date ;

create table STV2023060652__DWH.l_currency_operation
(
    hk_l_currency_operation_id integer not null,
    hk_currency_id int not null CONSTRAINT fk_l_currency_operation_h_currencies REFERENCES STV2023060652__DWH.h_currencies(hk_currency_id),
    hk_operation_id int not null CONSTRAINT fk_l_currency_operation_h_operations REFERENCES STV2023060652__DWH.h_operations(hk_operation_id),
    account_number_from varchar(10) not null,
    account_number_to varchar(10) not null,
    country varchar(150) not null,
    status varchar(20) not null,
    transaction_type varchar(30) not null,
    amount numeric(15,0) not null CONSTRAINT c_l_currency_operation_check_amount check(amount<>0),
    transaction_dt datetime not null,
    load_dt datetime not null,
    load_src varchar(60),
    constraint c_l_currency_operation_pk primary key (hk_l_currency_operation_id) ENABLED
)
order by load_dt
SEGMENTED BY hk_l_currency_operation_id all nodes
PARTITION BY load_dt::date ;

create table STV2023060652__DWH.l_currency_operation_date
(
    hk_l_currency_operation_date_id integer not null,
    hk_operation_id int not null CONSTRAINT fk_l_currency_operation_h_operations REFERENCES STV2023060652__DWH.h_operations(hk_operation_id),
    hk_currency_id int not null CONSTRAINT fk_l_currency_operation_h_currencies REFERENCES STV2023060652__DWH.h_currencies(hk_currency_id),
    transaction_date date not null,
    account_number_from varchar(10) not null,
    country varchar(150) not null,
    date_status varchar(20) not null,
    amount numeric(15,0) not null CONSTRAINT c_l_currency_operation_check_amount check(amount<>0),    
    load_dt datetime not null,
    load_src varchar(100),
    constraint c_l_currency_operation_pk primary key (hk_l_currency_operation_date_id) ENABLED
)
order by load_dt
SEGMENTED BY hk_l_currency_operation_date_id all nodes
PARTITION BY load_dt::date ;

drop table if exists STV2023060652__DWH.global_metrics;

CREATE TABLE STV2023060652__DWH.global_metrics
(
    id  IDENTITY ,
    date_update date NOT NULL,
    currency_from varchar(5) NOT NULL,
    amount_total numeric(14,2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account numeric(15,2) NOT NULL,
    cnt_accounts_make_transactions int NOT NULL,
    load_dt timestamp NOT NULL,
    load_src varchar(50),
    CONSTRAINT c_global_metrics_pk PRIMARY KEY (date_update, currency_from) ENABLED
)
order by date_update
SEGMENTED BY hash(date_update, currency_from) all nodes
PARTITION BY (global_metrics.date_update);
