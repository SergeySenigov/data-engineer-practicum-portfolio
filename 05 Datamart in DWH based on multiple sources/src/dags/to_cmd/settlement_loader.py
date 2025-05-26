from logging import Logger
from typing import List

from examples.to_cmd import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import datetime as dt

class SettlementObj(BaseModel):

    restaurant_id: str
    restaurant_name: str
    settlement_date: dt.date
    max_ts: dt.datetime
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float

class SettlementOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, settlement_threshold: int, limit: int) -> List[SettlementObj]:
        with self._db.client().cursor(row_factory=class_row(SettlementObj)) as cur:
            cur.execute(
                """
                select restaurant_id, restaurant_name, settlement_date, count(distinct order_key) orders_count, max(ts) max_ts,
                  sum(total_sum) orders_total_sum, sum(bonus_payment) orders_bonus_payment_sum, sum(bonus_grant) orders_bonus_granted_sum, 
                  sum(total_sum) *0.25 order_processing_fee, sum(total_sum) - sum(bonus_payment) - sum(total_sum) *0.25 restaurant_reward_sum
                from (
                  select dt.id, fps.order_id , fps.total_sum , fps.bonus_payment , fps.bonus_grant, dr.restaurant_id , dr.restaurant_name ,
                   dt."year" , dt."month", dt."date", do2.order_key , dt.ts,
                   dt."date" settlement_date
                  from dds.fct_product_sales fps 
                  left join dds.dm_orders do2 on do2.id = fps.order_id and do2.order_status = 'CLOSED'
                  left join dds.dm_restaurants dr on dr.id = do2.restaurant_id 
                  left join dds.dm_timestamps dt on dt.id = do2.timestamp_id 
                ) t
                group by restaurant_id, restaurant_name, settlement_date
                """, {
                    "threshold": settlement_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SettlementDestRepository:

    def insert_object(self, conn: Connection, settlement: SettlementObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, 
                    orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, 
                    %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s,
                    %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE 
                    set 
                      orders_count = EXCLUDED.orders_count,
                      orders_total_sum = EXCLUDED.orders_total_sum, 
                      orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum, 
                      orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum, 
                      order_processing_fee = EXCLUDED.order_processing_fee,
                      restaurant_reward_sum = EXCLUDED.restaurant_reward_sum   ;
                """,
                {
                    "restaurant_id": settlement.restaurant_id,
                    "restaurant_name": settlement.restaurant_name,
                    "settlement_date": settlement.settlement_date,
                    "orders_count": settlement.orders_count,
                    "orders_total_sum": settlement.orders_total_sum,
                    "orders_bonus_payment_sum": settlement.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": settlement.orders_bonus_granted_sum,
                    "order_processing_fee": settlement.order_processing_fee,
                    "restaurant_reward_sum": settlement.restaurant_reward_sum
                },
            )

class SettlementLoader:
    WF_KEY = "settlement_dds_to_cdm_workflow"
    WF_MAX_LOADED_TS_KEY = "max_loaded_ts"
    WF_LOADED_ORDERS_KEY = "loaded_orders"
    BATCH_LIMIT = 100  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dds = SettlementOriginRepository(pg_origin)
        self.cdm = SettlementDestRepository()
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
                   workflow_settings={self.WF_MAX_LOADED_TS_KEY: -1, self.WF_LOADED_ORDERS_KEY:0})
            self.log.info(f'wf_setting = {wf_setting}')

            # Вычитываю очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.WF_MAX_LOADED_TS_KEY]
            load_queue = self.dds.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} settlements to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняю объекты в базу dwh.
            for object in load_queue:
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
            wf_setting.workflow_settings[self.WF_LOADED_ORDERS_KEY] = sum([t.orders_count for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразую к строке, чтобы положить в БД.
            self.log.info(f'wf_setting = {wf_setting}')
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.WF_MAX_LOADED_TS_KEY]}")


