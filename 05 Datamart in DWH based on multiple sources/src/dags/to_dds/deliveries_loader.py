from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class DeliveryObj(BaseModel):
    id: int
    delivery_id: str 
    order_id: int
    address: str 
    delivery_ts: dt.datetime 

class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, _threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                   select 
                    t.id, 
                    t.delivery_id,
                    (select id from dds.dm_orders where dm_orders.order_key = t.order_id) order_id,
                    t.address,
                    t.delivery_ts
                   from stg.deliverysystem_deliveries t
                   WHERE id > %(threshold)s 
                   and exists (select id from dds.dm_orders where dm_orders.order_key = t.order_id)
                   ORDER BY id ASC LIMIT %(limit)s
                    ; --Обрабатываю только одну пачку объектов.
                """, {
                    "threshold": _threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveriesDestRepository:

    def insert_object(self, conn: Connection, delivery: DeliveryObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, delivery_ts, order_id, address)
                    VALUES (%(delivery_id)s, %(delivery_ts)s, %(order_id)s, %(address)s)
                        ;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "delivery_ts": delivery.delivery_ts,
                    "order_id": delivery.order_id,
                    "address": delivery.address
                },
            )


class DeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 5000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DeliveriesOriginRepository(pg_origin)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def load_data(self):
        # Открываю транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываю состояние загрузки
            # Если настройки еще нет, создаю ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            self.log.info(f'wf_setting = {wf_setting}')
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываю очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f'last_loaded = {last_loaded}')
            self.log.info(f'BATCH_LIMIT = {self.BATCH_LIMIT}')
            load_queue = self.stg.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняю объекты в базу dwh в dds.
            for object in load_queue:
                try:
                    self.dds.insert_object(conn, object)
                except Exception as err:
                    print(object)
                    print('Error =', err)   
                    raise  

            # Сохраняю прогресс.
            # Пользуеюсь тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразую к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
