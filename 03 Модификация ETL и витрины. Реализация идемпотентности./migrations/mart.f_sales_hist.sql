delete from mart.f_sales
where f_sales.date_id in
    (select d_calendar.date_id from mart.d_calendar where mart.d_calendar.date_actual < '{{ds}}');

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date < '{{ds}}';