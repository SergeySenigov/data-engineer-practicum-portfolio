## **Цели проекта**  

- Модифицировать процессы в пайплайне, чтобы они соответствовали новым задачам бизнеса. Обеспечить обратную совместимость.

- Создать обновленную витрину данных для исследования возвращаемости клиентов.

- Модифицировать ETL процесс, чтобы поддерживалась идемпотентность.

## **Используемые технологии**
AirFlow      
SQL  
PostgreSQL  
cloudbeaver  
bash    
psycopg2    
pandas
SQLAlchemy  
PostgresOperator  
BashOperator  

## **Постановка задачи**
1. Команда разработки добавила в систему заказов магазина функционал отмены заказов и возврата средств (refunded). Требуется обновить процессы в пайплайне для учета нового функционала: использовать в витрине mart.f_sales статусы shipped и refunded. Все данные в витрине следует считать shipped.

2. Вычислить метрики customer retention в дополнительной витрине.  
Наполнить витрину.  
Эта витрина должна отражать следующую информацию:
* Рассматриваемый период — week.  
* Возвращаемость клиентов: 
    - new — количество клиентов, которые оформили один заказ за рассматриваемый период;
    - returning — количество клиентов, которые оформили более одного заказа за рассматриваемый период;
    - refunded — количество клиентов, которые вернули заказ за рассматриваемый период.
* Доход (revenue) и refunded для каждой категории покупателей.
С помощью новой витрины можно будет выяснить, какие категории товаров лучше всего удерживают клиентов.

Требуемая схема витрины:  
- new_customers_count — кол-во новых клиентов (тех, которые сделали только один 
заказ за рассматриваемый промежуток времени).
- returning_customers_count — кол-во вернувшихся клиентов (тех,
которые сделали только несколько заказов за рассматриваемый промежуток времени).
- refunded_customer_count — кол-во клиентов, оформивших возврат за 
рассматриваемый промежуток времени.
- period_name — weekly.
- period_id — идентификатор периода (номер недели или номер месяца).
- item_id — идентификатор категории товара.
- new_customers_revenue — доход с новых клиентов.
- returning_customers_revenue — доход с вернувшихся клиентов.
- customers_refunded — количество возвратов клиентов.   

3. Модифицировать процесс для соответствия идемпотентности. Перезапустить пайплайн и убедиться, что после перезапуска не появилось дубликатов в витринах mart.f_sales и mart.f_customer_retention.  

## **Реализация**

Скрипты изменения и создания объектов БД, миграции данных в новую структуру в [migrations](migrations)

Обновленный скрипт с описанием DAG "realization.py" в папке [src/dags](src/dags)

**Этап 1 - Модифицировать процессы в пайплайне**

Так как в инкрементальных данных появилось новое поле status, то добавим его в таблицу staging.user_order_log. 

Сделаем новое поле status обязательным и заполним старые записи значением 'shipped'.

Скрипт /migrations/1_add_staging_uol_field_status.sql:
```sql
alter table staging.user_order_log add column status varchar(30) default 'shipped' not null;
```

Добавим в витрину mart.f_sales новое поле refund_flag типа boolean для контроля заполнения

Скрипт /migrations/2_add_mart_f_sales_field_refund_flag.sql:
```sql
alter table mart.f_sales add column refund_flag boolean default false not null;
```

Не будем изменять в коде алгоритм заполнения refunded в слое staging (менять значения на минусы), чтобы сохранить полностью оригинальные данные.

Будем передавать в запросе формирования данных витрины из staging.user_order_log в mart.f_sales суммы 
в поля quantity и payment_amount для записей со статусом refunded с минусами. 

Для чего сделаем новый скрипт. 
Чтобы данные можно было обновлять за выборочные дни без удаления и дублирования, выполним в этом же скрипте запрос удаления ранее загруженных данных за эту дату.

Так как данные за конкретную дату грузятся только этим процессом и не агрегируются в витрине, то нарушений не будет.

Для инкрементов и исторических данных скрипт будем немного различаться, так как в первом случае грузятся данные только за одну дату (условная дата запуска - ds), во втором - за произвольные даты до даты ds.


[Скрипт для инкрементов /migrations/mart.f_sales_inc.sql](migrations/mart.f_sales_inc.sql)
```sql
delete from mart.f_sales
where f_sales.date_id in
    (select dc.date_id from mart.d_calendar where mart.d_calendar.date_actual = '{{ds}}');

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, refund_flag)
select dc.date_id, item_id, customer_id, city_id, 
quantity * (case when uol.status = 'refunded' then -1 else 1 end) quantity, 
payment_amount * (case when uol.status = 'refunded' then -1 else 1 end) payment_amount, 
(uol.status = 'refunded') 
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';
```

[Скрипт для исторических данных /migrations/mart.f_sales_hist.sql](migrations/mart.f_sales_hist.sql)  
В исторических данных нет строк со статусом refunded, можно не подставлять минусы, поле refund_flag заполнится по умолчанию значением false.

```sql
delete from mart.f_sales
where f_sales.date_id in
    (select d_calendar.date_id from mart.d_calendar where mart.d_calendar.date_actual < '{{ds}}');

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount 
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date < '{{ds}}';
```

В файле настройки DAG сделаем для загрузки исторических данных второй DAG, в котором создадим свой набор задач (с префиксом h_).

Изменим вызов в DAG
```sql
update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_inc.sql", # было f_sales.sql
        parameters={"date": {business_dt}})
```

Меняем работу функций f_upload_data_to_staging, f_upload_data_to_staging_hist в DAG, чтобы можно было обновлять за выборочные дни без удаления и дублирования: выполним запрос удаления ранее загруженных данных за эту дату в таблице user_order_log:

Для инкрементов:
```sql
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date = '{date}'" 
engine.execute(str_del)
```

Для исторических данных
```sql
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date < '{date}'" 
engine.execute(str_del)
```

В процедуре загрузки исторических данных f_upload_data_to_staging_hist нужно подставить в URL файла вместо increment_id значение 
report_id, полученное ранее в f_get_report

```sql
increment_id = ti.xcom_pull(key='report_id')
```

Здесь же для диагностики выведем сгруппированные данные, которые грузим из файла в user_order_log
```sql
print (df.groupby(['date_time', 'status']).agg({'status': 'count', 'quantity': 'sum', 'payment_amount': 'sum'}))
```


**Этап 2 - Реализовать новую витрину**

Создаем витрину по требуемой структуре.
Сделаем внешний ключ по item_id к mart.d_item.

```sql
drop table if exists mart.f_customer_retention ;

create table mart.f_customer_retention (
	id serial4 PRIMARY KEY,  
    new_customers_count int4 not null, 
    returning_customers_count int4 not null, 
    refunded_customer_count int4 not null, 
    period_name varchar(20) not null, 
    period_id varchar(20) not null, 
    item_id int4 not null, 
    new_customers_revenue numeric(12,2) not null, 
    returning_customers_revenue numeric(12,2) not null,
    customers_refunded numeric(12,0) not null,

    CONSTRAINT f_customer_retention_item_id_fkey FOREIGN KEY (item_id)
        REFERENCES mart.d_item (item_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION);

	CREATE INDEX IF NOT EXISTS f_cr2
    ON mart.f_customer_retention USING btree
    (item_id ASC NULLS LAST)
    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS f_cr3
    ON mart.f_customer_retention USING btree
    (period_id ASC NULLS LAST)
    TABLESPACE pg_default;
	
    CREATE INDEX IF NOT EXISTS f_cr4
    ON mart.f_customer_retention USING btree
    (period_name ASC NULLS LAST)
    TABLESPACE pg_default;
```

Для заполнения по историческим данным сделаем запрос из 
[/migrations/mart.f_customer_retention_hist.sql](migrations/mart.f_customer_retention_hist.sql)

Для заполнения по историческим данным сделаем запрос из 
[/migrations/mart.f_customer_retention_inc.sql](migrations/mart.f_customer_retention_inc.sql)


В оба DAG добавим соответствующие задачи типа PostgresOperator: 
```sql
    h_update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention_hist.sql",
        parameters={"date": {business_dt}} )


    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention_inc.sql",
        parameters={"date": {business_dt}} )
```

Внесем их в дерево выполнения:
```sql
    (
            h_print_info_task 
            >> h_generate_report
            >> h_get_report
            >> h_upload_user_order
            >> [h_update_d_item_table, h_update_d_city_table, h_update_d_customer_table]
            >> h_null_task
            >> [h_update_f_sales, h_update_f_customer_retention]
    )

...

    (
            print_info_task
             >> generate_report
             >> get_report
             >> get_increment
             >> upload_user_order_inc
             >> [update_d_item_table, update_d_city_table, update_d_customer_table]
             >> null_task
             >> [update_f_sales, update_f_customer_retention]
    )
```
    

**Этап 3 - Поддержика идемпотентности**

В процессе этапов 1 и 2 были реализованы процедуры очистки и заполнения таблиц таким образом, чтобы было возможно независимое удаление и восстановление информации за отдельные дни без затрагивания информации за другие дни.

В частности изменили работу функций f_upload_data_to_staging, f_upload_data_to_staging_hist в DAG, чтобы можно было обновлять за выборочные дни без удаления и дублирования: выполняется запрос удаления ранее загруженных данных за эту дату в таблице user_order_log:

Для инкрементов:
```sql
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date = '{date}'" 
engine.execute(str_del)
```

Для исторических данных
```sql
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date < '{date}'" 
engine.execute(str_del)
```

Для витрины mart.f_customer_retention выполняется удаление данных за соответствующую неделю 

```sql
delete from mart.f_customer_retention 
where f_customer_retention.period_id =
   (select substr(d_calendar.week_of_year_iso, 1, 8) from mart.d_calendar where d_calendar.date_actual = '{{ds}}' ) ;
...
```

и их восстановление с использованием новых данных отчетного дня.

```sql
    ...
   	from staging.user_order_log uol2 
	join mart.d_calendar on uol2.date_time::date = d_calendar.date_actual 
			and '{{ ds }}' between d_calendar.first_day_of_week and d_calendar.last_day_of_week
```
