from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class ProductObj(BaseModel):
    id: int
    restaurant_id: str
    product_id: str
    product_name: str
    product_price: float
    active_from: dt.datetime

class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, products_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                   select 
                    id, 
                    (select id from dds.dm_restaurants r where r.restaurant_id = t.restaurant_id ) restaurant_id,
                    product_json::JSON->>'id' product_id,
                    product_json::JSON->>'name' product_name,
                    (product_json::JSON->>'price')::float product_price, 
                    update_ts as active_from
                   from (
                     select 
                         id AS id,
                         object_id AS order_id,
                         (object_value::json->>'restaurant')::json->>'id' AS restaurant_id,
                         update_ts AS update_ts,
                         json_array_elements((object_value::json->>'order_items')::JSON) product_json
                    FROM (select * from stg.ordersystem_orders t WHERE id > %(threshold)s LIMIT %(limit)s) t
                     --Пропускаю те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используется в качестве курсора.
                    ) t
                    ; --Обрабатываю только одну пачку объектов.
                """, {
                    "threshold": products_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductsDestRepository:

    def insert_object(self, conn: Connection, product: ProductObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (product_id) DO UPDATE
                    SET
                        active_from = EXCLUDED.active_from
                    where dm_products.active_from > EXCLUDED.active_from
                        ;
                """,
                {
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": '2099-12-31 00:00:00'
                },
            )




class ProductsLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = ProductsOriginRepository(pg_origin)
        self.dds = ProductsDestRepository()
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
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for object in load_queue:
                #print(object)
                self.dds.insert_object(conn, object)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
