from logging import Logger
from typing import List

from examples.to_cmd import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class CourierLedgerObj(BaseModel):

    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    max_ts: dt.datetime
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float

class CourierLedgerOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, _threshold: int, limit: int) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            cur.execute(
                """
                with ss as (
                select 
                  fcd.courier_id,
                  dt.year as settlement_year,
                  dt.month as settlement_month,
                  max(dt.ts) max_ts,
                  count(1) as orders_count,
                  sum(delivery_sum) as orders_total_sum,
                  avg(fcd.rate) as rate_avg,
                  case when avg(fcd.rate) < 4 then least(100*count(1), sum(delivery_sum)*0.05)
                       when avg(fcd.rate) < 4.5 then least(150*count(1), sum(delivery_sum)*0.07)
                       when avg(fcd.rate) < 4.9 then least(175*count(1), sum(delivery_sum)*0.08)
                       else least(200*count(1), sum(delivery_sum)*0.10) end as courier_order_sum,
                  sum(fcd.tip_sum) courier_tips_sum
                from dds.fct_courier_deliveries fcd
                   join dds.dm_deliveries dd on dd.id = fcd.delivery_id
                   join dds.dm_orders do2 on do2.id = dd.order_id
                   join dds.dm_timestamps dt on dt.id = do2.timestamp_id
                group by fcd.courier_id, dt.year, dt.month ) 
                select 
                  (select courier_id from dds.dm_couriers where dm_couriers.id = ss.courier_id) as courier_id,
                  (select courier_name from dds.dm_couriers where dm_couriers.id = ss.courier_id) as courier_name,
                  settlement_year,
                  settlement_month,
                  max_ts,
                  orders_count,
                  orders_total_sum,
                  rate_avg,
                  orders_total_sum*0.25 as order_processing_fee,
                  courier_order_sum,
                  courier_tips_sum,
                  courier_order_sum + courier_tips_sum*0.95 as courier_reward_sum
                from ss
                """, {
                    "threshold": _threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CourierLedgerDestRepository:

    def insert_object(self, conn: Connection, courier_ledger: CourierLedgerObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, 
                    orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum,
                    courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, 
                    %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s,
                    %(courier_tips_sum)s, %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE 
                    set 
                      courier_name = EXCLUDED.courier_name,
                      orders_count = EXCLUDED.orders_count, 
                      orders_total_sum = EXCLUDED.orders_total_sum, 
                      rate_avg = EXCLUDED.rate_avg, 
                      order_processing_fee = EXCLUDED.order_processing_fee,
                      courier_order_sum = EXCLUDED.courier_order_sum,
                      courier_tips_sum = EXCLUDED.courier_tips_sum,
                      courier_reward_sum = EXCLUDED.courier_reward_sum
                         ;
                """,
                {
                    "courier_id": courier_ledger.courier_id,
                    "courier_name": courier_ledger.courier_name,
                    "settlement_year": courier_ledger.settlement_year,
                    "settlement_month": courier_ledger.settlement_month,
                    "orders_count": courier_ledger.orders_count,
                    "orders_total_sum": courier_ledger.orders_total_sum,
                    "rate_avg": courier_ledger.rate_avg,
                    "order_processing_fee": courier_ledger.order_processing_fee,
                    "courier_order_sum": courier_ledger.courier_order_sum,
                    "courier_tips_sum": courier_ledger.courier_tips_sum,
                    "courier_reward_sum": courier_ledger.courier_reward_sum
                },
            )

class CourierLedgerLoader:
    WF_KEY = "courier_ledger_dds_to_cdm_workflow"
    WF_MAX_LOADED_TS_KEY = "max_loaded_ts"
    WF_LOADED_LEDGER_LINES_KEY = "loaded_ledger_lines"
    BATCH_LIMIT = 100  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = CourierLedgerOriginRepository(pg_origin)
        self.cdm = CourierLedgerDestRepository()
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
            if not wf_setting:
                 wf_setting = EtlSetting(id=0, 
                     workflow_key=self.WF_KEY, 
                     workflow_settings={self.WF_MAX_LOADED_TS_KEY: -1, self.WF_LOADED_LEDGER_LINES_KEY: 0})
            self.log.info(f'wf_setting = {wf_setting}')

            # Вычитываю очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.WF_MAX_LOADED_TS_KEY]
            load_queue = self.dds.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} courier ledger lines to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняю объекты в базу dwh.
            for object in load_queue:
                #print(object)
                try:
                    self.cdm.insert_object(conn, object)
                except Exception as err:
                    print(object)
                    print('insert_object Error =', err)   
                    raise  

            # Сохраняю прогресс.
            # Пользуюсь тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.WF_MAX_LOADED_TS_KEY] = max([t.max_ts for t in load_queue])
            wf_setting.workflow_settings[self.WF_LOADED_LEDGER_LINES_KEY] = len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразую к строке, чтобы положить в БД.
            self.log.info(f'wf_setting = {wf_setting}')
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.WF_MAX_LOADED_TS_KEY]}")


