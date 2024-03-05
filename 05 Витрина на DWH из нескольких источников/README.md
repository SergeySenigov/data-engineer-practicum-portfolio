## **Цели проекта**  

- Усовершенствовать хранилище данных: добавить новый источник и витрину. 
- Связать данные из нового источника с данными в хранилище.  
- Реализовать витрину для расчётов с курьерами.  

## **Используемые технологии и инструменты**
AirFlow  
PostgreSQL    
MongoDB Compass  
cloudbeaver    
Jupyter Notebook  
bash   
PgConnect  
MongoConnect  
pendulum  
SQLAlchemy  
PostgresHook  

## **Постановка задачи**

Реализовать витрину для расчётов с курьерами. В ней требуется рассчитать суммы оплаты каждому курьеру за предыдущий месяц.  Расчётным числом является 10-е число каждого месяца. Из витрины бизнес будет брать нужные данные в готовом виде.
Отчёт собирается по дате заказа. Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте нужно ориентироваться на дату заказа, а не дату доставки.  
Необходимо:
Изучить возможные источники и их внутреннюю модель хранения данных, а также технологии, которые используются для получения данных из этих источников.
На основе требований, целей дальнейшего использования, информации об источниках нужно спроектировать многослойную модель DWH.
Оформить необходимую документацию для передачи результатов заказчику.  

## **Реализация**

Исследование API получения данных о доставках содержится 
 в ноутбуке [Inspect API And Data Model.ipynb](<Inspect API And Data Model.ipynb>)


Cтруктура витрины расчётов с курьерами в CDM
--- 

При проектировании DWH вырал метод - от структуры необходимых данных витрины – вниз.
Вначале изучаю, какие данные должны быть в витрине и спроектирую таблицу для нее.

В задании указана требуемая структура данных витрины и имя таблицы. На ее основании создал таблицу витрины `cdm.dm_courier_ledger` в слое CDM.

```sql
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
```

Для отслеживания процессов загрузки создал в схеме CDM таблицу srv_wf_settings:
```sql
CREATE TABLE cdm.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key UNIQUE (workflow_key)
);
```

Cтруктура объектов для расчётов с курьерами в DDS

--- 

Чтобы собрать описанную выше витрину мне потребуется:
- таблица измерений с данными о курьерах - оттуда возьму courier_name.
- таблица измерений с общими данными о доставках - для хранения адреса, даты доставки. 
Сделаю в этой таблице также ссылку на заказ (имеющаяся таблица dm_orders), чтобы не вносить ее в таблиц фактов, так как в будущем могут быть другие таблицы фактов использующие эту общую таблицу с данными доставки.
- таблица фактов с данными доставок с привязкой курьеров - со ссылками на курьеры и доставки. 

Так как известен заказ доставки, а таблица заказов в DDS уже содержит ссылку на его дату, то не буду повторно записывать в DDS дату заказа из системы доставок.

Ниже код создания описанных таблиц
Таблица измерений с данными о курьерах 
```sql
DROP TABLE if exists dds.dm_couriers;

CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_pk PRIMARY KEY (id),
	CONSTRAINT dm_couriers_courier_id_uindex UNIQUE (courier_id)
);
```

Таблица измерений с данными о доставках. Имеет внешний ключ на таблицу измерений заказов.
```sql
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
```

Таблицу фактов с данными о доставках по курьерам. Имеет внешние ключи на таблицы измерений курьеров и доставок.
```sql
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
```


Cтруктура объектов для расчётов с курьерами в STG
---

Для заполнения DDS потребуются "сырые" данные о доставках и курьерах. Данные о ресторанах не требуются. 
Потребуются следующие данные:
- код и имя курьера
- код, дата/время, код курьера, код заказа (для даты заказа), выставленная оценка курьеру, сумма заказа, сумма чаевых каждой доставки

Данные заказов уже имеются в системе.

Создал в STG таблицу с данными курьеров. Первичный ключ id во всех таблицах STG будет автогенерируемый.
Наложил проверку уникальности для id курьера из системы доставки (`courier_id`). 
```sql
DROP TABLE if exists stg.deliverysystem_couriers;

CREATE TABLE stg.deliverysystem_couriers (
	id serial4 NOT null ,
	courier_id varchar not null, 
	"name" varchar NOT null,
	
	CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT deliverysystem_couriers_courier_id_uindex UNIQUE (courier_id)
);
```

Создаю в STG таблицу с данными доставок. 
Делаю проверку уникальности для id доставки из системы доставки (`delivery_id`). 

```sql
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
```

Данные по ресторанам (id, name) уже имеются в слое STG в таблице `ordersystem_restaurants`, ее создавать и заполнять не буду.

Из трех исследованных API беру данные:
- из /couriers записывать в stg.deliverysystem_couriers
- из /deliveries записывать в stg.deliverysystem_deliveries


Скрипты создания объектов БД в папке [ddl](ddl)

Код для DAG:  
**Слой STG**    
[src/dags/stg/delivery_system_dag](src/dags/stg/delivery_system_dag):  
- [src/dags/stg/delivery_system_dag/project_5_dag.py](src/dags/stg/delivery_system_dag/project_5_dag.py)  
- [src/dags/stg/delivery_system_dag/couriers_loader.py](src/dags/stg/delivery_system_dag/couriers_loader.py)  
- [src/dags/stg/delivery_system_dag/deliveries_loader.py](src/dags/stg/delivery_system_dag/deliveries_loader.py)  


**Слой DDS**  
[src/dags/to_dds](src/dags/to_dds):  
-  [src/dags/to_dds/from_stg_to_dds_dag.py](src/dags/to_dds/from_stg_to_dds_dag.py)  
- [src/dags/to_dds/courier_deliveries_loader.py](src/dags/to_dds/courier_deliveries_loader.py)  
- [src/dags/to_dds/couriers_loader.py](src/dags/to_dds/couriers_loader.py)  
- [src/dags/to_dds/deliveries_loader.py](src/dags/to_dds/deliveries_loader.py)  


**Слой CDM**  
[src/dags/to_cmd](src/dags/to_cmd):  
-  [src/dags/to_cmd/from_dds_to_cdm_dag.py](src/dags/to_cmd/from_dds_to_cdm_dag.py)  
-  [src/dags/to_cmd/courier_ledger_loader.py](src/dags/to_cmd/courier_ledger_loader.py)  
