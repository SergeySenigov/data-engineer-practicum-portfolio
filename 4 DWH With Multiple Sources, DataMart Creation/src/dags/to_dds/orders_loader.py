from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class OrderObj(BaseModel):
    id: int
    order_id: str
    #bonus_grant: float 
    #bonus_payment: float
    #cost: float
    order_ts: dt.datetime
    final_status: str
    user_id: int
    restaurant_id: int
    timestamp_id: int
    #update_ts: dt.datetime

class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, orders_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    select id, order_id, final_status, t.order_ts,
                      (select id from dds.dm_users du where du.user_id = t.user_id) user_id,
                      (select id from dds.dm_restaurants dr where dr.restaurant_id = t.restaurant_id) restaurant_id,
                      (select id from dds.dm_timestamps dt where dt.ts = t.order_ts) timestamp_id
                    from (
                       select 
                       id AS id,
                       object_id AS order_id,
                       --(object_value::json->>'bonus_grant')::float AS bonus_grant,
                       --(object_value::json->>'bonus_payment')::float  AS bonus_payment,
                       --(object_value::json->>'cost')::float AS cost,
                       (object_value::json->>'date')::timestamp AS order_ts,
                       object_value::json->>'final_status' AS final_status,
                       (object_value::json->>'user')::json->>'id' AS user_id,
                       (object_value::json->>'restaurant')::json->>'id' AS restaurant_id,
                       update_ts AS update_ts
                       FROM stg.ordersystem_orders
                       WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                       ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                       LIMIT %(limit)s --Обрабатываем только одну пачку объектов.
                    ) t 
                    where exists (select id from dds.dm_timestamps dt where dt.ts = t.order_ts) /*!!*/ ;
                """, {
                    "threshold": orders_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrdersDestRepository:

    def insert_object(self, conn: Connection, order: OrderObj) -> None:

        # print (order)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                """,
                {
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "order_key": order.order_id,
                    "order_status": order.final_status,
                },
            )


class OrdersLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.dds = OrdersDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def load_data(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            self.log.info(f'wf_setting = {wf_setting}')
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f'last_loaded = {last_loaded}')
            self.log.info(f'BATCH_LIMIT = {self.BATCH_LIMIT}')
            load_queue = self.origin.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
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
