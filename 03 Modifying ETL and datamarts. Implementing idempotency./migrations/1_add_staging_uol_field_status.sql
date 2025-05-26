alter table staging.user_order_log add column status varchar(30) default 'shipped' not null;
