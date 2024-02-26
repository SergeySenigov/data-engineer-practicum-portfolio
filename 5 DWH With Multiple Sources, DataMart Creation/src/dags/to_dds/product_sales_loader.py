from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class SalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, sales_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor(row_factory=class_row(SalesObj)) as cur:
            cur.execute(
                """
with ss as (
  select 
    (t2.product_json::json->>'bonus_payment')::float bonus_payment, 
    (t2.product_json::json->>'bonus_grant')::float bonus_grant,
    t2.order_id,
    (t2.product_json::json->>'product_id')::text product_id
  from 
  (
    select --be.id, 
      be.event_type, json_array_elements((be.event_value::json->>'product_payments')::JSON) product_json,
      (be.event_value::json->>'order_id')::text order_id
    from stg.bonussystem_events be 
    where be.event_type = 'bonus_transaction'
  ) t2
) 
                    select 
                    t.id, 
                    (select id from dds.dm_products p where p.product_id = (t.product_json::JSON->>'id')) product_id,
                    (select id from dds.dm_orders o where o.order_key = t.order_id) order_id,
                    (product_json::JSON->>'quantity')::int count,
                    (product_json::JSON->>'price')::float price,
                    (product_json::JSON->>'quantity')::int * (product_json::JSON->>'price')::float total_sum,
                    ss.bonus_payment,
                    ss.bonus_grant
                   from (
                     select 
                         id AS id,
                         object_id AS order_id,
                         json_array_elements((object_value::json->>'order_items')::JSON) product_json
                         FROM (select * from stg.ordersystem_orders WHERE id > %(threshold)s ORDER BY id ASC LIMIT %(limit)s) t
                    where object_value::json->>'final_status' = 'CLOSED' -- только успешно закрытые
                     --Пропускаю те объекты, которые уже загрузили.
                     --Обязательна сортировка по id, т.к. id используется в качестве курсора.
                    ) t, ss
                    where ss.order_id =  t.order_id 
                    and ss.product_id = t.product_json::JSON->>'id' 
                    and exists (select id from dds.dm_orders o where o.order_key = t.order_id)
                    ; --Обрабатываю только одну пачку объектов.
                """, {
                    "threshold": sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SalesDestRepository:

    def insert_object(self, conn: Connection, sale: SalesObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                        ;
                """,
                {
                    "product_id": sale.product_id,
                    "order_id": sale.order_id,
                    "count": sale.count,
                    "price": sale.price,
                    "total_sum": sale.total_sum,
                    "bonus_payment": sale.bonus_payment,
                    "bonus_grant": sale.bonus_grant
                },
            )




class ProductSalesLoader:
    WF_KEY = "sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 5000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = SalesOriginRepository(pg_origin)
        self.dds = SalesDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def load_data(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, создаю ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            self.log.info(f'wf_setting = {wf_setting}')
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f'last_loaded = {last_loaded}')
            self.log.info(f'BATCH_LIMIT = {self.BATCH_LIMIT}')
            load_queue = self.stg.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for object in load_queue:
                try:
                    self.dds.insert_object(conn, object)
                except Exception as err:
                    print(object)
                    print('Error =', err)   
                    raise  

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
