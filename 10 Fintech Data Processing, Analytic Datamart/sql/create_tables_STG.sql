drop table if exists STV2023060652__STAGING.transactions cascade;

create table STV2023060652__STAGING.transactions 
(
    operation_id UUID ,
    account_number_from varchar(10),
    account_number_to varchar(10),
    currency_code varchar(5),
    country varchar(150),
    status varchar(20),
    transaction_type varchar(30),
    amount numeric(15,0),
    transaction_dt timestamp not null,
    file_name varchar(150)
)
order by transaction_dt, operation_id, currency_code
segmented by hash(transactions.transaction_dt::date, transactions.operation_id, transactions.currency_code) all nodes KSAFE 1
PARTITION BY transaction_dt::date ;

create PROJECTION STV2023060652__STAGING.transactions_td_opid
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt,
 file_name
)
as
 select transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt,
        transactions.file_name
 from STV2023060652__STAGING.transactions
 order by transactions.transaction_dt,
          transactions.operation_id
 segmented by hash(transactions.transaction_dt, transactions.operation_id) all nodes KSAFE 1;


drop table if exists STV2023060652__STAGING.currencies cascade; 

create table STV2023060652__STAGING.currencies
(
    date_update timestamp not null,
    currency_code varchar(5),
    currency_code_with varchar(5),
    currency_with_div numeric(12,5),
    file_name varchar(150)
)
order by date_update, currency_code
segmented by hash(currencies.date_update) all nodes KSAFE 1
PARTITION BY date_update::date ;

create PROJECTION STV2023060652__STAGING.currencies_du_cc
(
 date_update,
 currency_code,
 currency_code_with,
 currency_with_div,
 file_name)
as
 select currencies.date_update,
  currencies.currency_code,
  currencies.currency_code_with,
  currencies.currency_with_div,
  currencies.file_name
 from STV2023060652__STAGING.currencies
 order by currencies.date_update, currency_code, currency_code_with
 segmented by hash(currencies.date_update, currencies.currency_code) all nodes KSAFE 1;

