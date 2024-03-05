from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class CourierDeliveryObj(BaseModel):
    id: int
    delivery_id: str 
    courier_id: str
    rate: int 
    delivery_sum: float 
    tip_sum: float  

class CourierDeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, _threshold: int, limit: int) -> List[CourierDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(CourierDeliveryObj)) as cur:
            cur.execute(
                """
                   select 
                    t.id,
                    (select id from dds.dm_couriers where dm_couriers.courier_id = t.courier_id) courier_id,
                    dd.id delivery_id,
                    t.rate,
                    t.delivery_sum,
                    t.tip_sum
                   from stg.deliverysystem_deliveries t
                     join dds.dm_deliveries dd on dd.delivery_id = t.delivery_id
                   WHERE t.id > %(threshold)s 
                   ORDER BY t.id ASC LIMIT %(limit)s
                    ; --Обрабатываю только одну пачку объектов.
                """, {
                    "threshold": _threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CourierDeliveriesDestRepository:

    def insert_object(self, conn: Connection, delivery: CourierDeliveryObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_courier_deliveries(courier_id, delivery_id, rate, delivery_sum, tip_sum)
                    VALUES (%(courier_id)s, %(delivery_id)s, %(rate)s, %(delivery_sum)s, %(tip_sum)s)
                        ;
                """,
                {
                    "courier_id": delivery.courier_id,
                    "delivery_id": delivery.delivery_id,
                    "rate": delivery.rate,
                    "delivery_sum": delivery.delivery_sum,
                    "tip_sum": delivery.tip_sum
                },
            )


class CourierDeliveriesLoader:
    WF_KEY = "courier_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 2000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = CourierDeliveriesOriginRepository(pg_origin)
        self.dds = CourierDeliveriesDestRepository()
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
            load_queue = self.stg.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} courier deliveries to load.")
            self.log.info(f'BATCH_LIMIT = {self.BATCH_LIMIT}')
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняю объекты в базу dwh.
            for object in load_queue:
                #print(object)
                try:
                    self.dds.insert_object(conn, object)
                except Exception as err:
                    print(object)
                    print('Error =', err)   
                    raise  

            # Сохраняю прогресс.
            # Пользуюсь тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразую к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
