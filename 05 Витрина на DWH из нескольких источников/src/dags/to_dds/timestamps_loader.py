from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class TimestampObj(BaseModel):
    id: int
    order_id: str
    bonus_grant: float 
    bonus_payment: float
    cost: float
    order_dt: dt.datetime
    final_status: str
    user_id: str
    update_ts: dt.datetime

class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, orders_threshold: int, limit: int) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    select  
                       id AS id,
                       object_id AS order_id,
                       (object_value::json->>'bonus_grant')::float AS bonus_grant,
                       (object_value::json->>'bonus_payment')::float  AS bonus_payment,
                       (object_value::json->>'cost')::float  AS cost,
                       (object_value::json->>'date')::timestamp AS order_dt,
                       object_value::json->>'final_status' AS final_status,
                       (object_value::json->>'user')::json->>'id' AS user_id,
                       update_ts AS update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаю те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используется в качестве курсора.
                    LIMIT %(limit)s ; --Обрабатываю только одну пачку объектов.
                """, {
                    "threshold": orders_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestampsDestRepository:

    def insert_object(self, conn: Connection, order: TimestampObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                """,
                {
                    "ts": order.order_dt,
                    "year": order.order_dt.strftime('%Y'),
                    "month": order.order_dt.strftime('%m'),
                    "day": order.order_dt.strftime('%d'),
                    "time": order.order_dt.strftime('%H:%M:%S'),
                    "date": order.order_dt.strftime('%Y-%m-%d'),
                },
            )




class TimestampsLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampsOriginRepository(pg_origin)
        self.dds = TimestampsDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def load_data(self):
        # открываю транзакцию.
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
            load_queue = self.origin.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняю объекты в базу dwh.
            for object in load_queue:
                self.dds.insert_object(conn, object)

            # Сохраняю прогресс.
            # Пользуюсь тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразую к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
