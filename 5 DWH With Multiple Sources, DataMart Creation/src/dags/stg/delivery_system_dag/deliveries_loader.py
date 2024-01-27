from logging import Logger
from typing import List

from examples.stg import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import requests
import json
import datetime as dt


class DeliveryObj(BaseModel):
    delivery_id: int = None
    order_id: str = None
    order_ts: dt.datetime = None
    courier_id: str = None
    address: str = None
    delivery_ts: dt.datetime = None
    rate: int = None
    delivery_sum: float = None
    tip_sum: float = None 

class DeliveriesOriginRepository():
    LOAD_BATCH_SIZE = 30

    def __init__(self, url) -> None:
        self.url = url 

    def list_deliveries(self, deliveries_threshold) -> List[DeliveryObj]:

        objs = list()
        request_headers = {'X-Nickname': 'senigov', 'X-Cohort': 'XX', 'X-API-KEY': 'XXXXXX'}

        offset = 0

        while True:
            deliveries_parameters = {'limit': self.LOAD_BATCH_SIZE,  'offset': offset, 
            'sort_field': 'date', 'sort_direction': 'asc',
            'from': deliveries_threshold}

            rs = requests.get(self.url, headers = request_headers, params = deliveries_parameters)
            print('offset =', offset, ', status_code =', rs.status_code, ', lenght is', len(json.loads(rs.content)))

            if len(json.loads(rs.content)) == 0 :
                print ('Length = 0, exiting while loop')
                break 

            if rs.status_code != 200:
                raise Exception ('Request error, status code = ' + str(rs.status_code))

            for delivery in json.loads(rs.content):
                obj = DeliveryObj()
                obj.delivery_id = delivery['delivery_id']
                obj.order_id = delivery['order_id']
                obj.order_ts = delivery['order_ts']
                obj.courier_id = delivery['courier_id']
                obj.address = delivery['address']
                obj.delivery_ts = delivery['delivery_ts']
                obj.rate = delivery['rate']
                obj.delivery_sum = delivery['sum']
                obj.tip_sum = delivery['tip_sum']
                objs.append(obj)

            offset += self.LOAD_BATCH_SIZE
            #break 

        return objs


class DeliveriesDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            r = cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, 
                      rate, delivery_sum, tip_sum)
                    VALUES (%(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, 
                      %(rate)s, %(delivery_sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        delivery_sum = EXCLUDED.delivery_sum,
                        tip_sum = EXCLUDED.tip_sum
                        ;
                """,
                {
                    "order_id": delivery.order_id,
                    "order_ts": delivery.order_ts,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "delivery_sum": delivery.delivery_sum,
                    "tip_sum": delivery.tip_sum
                },
            )


class DeliveriesLader:
    WF_KEY = "deliveries_http_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    NUM_LOADED_KEY = "number_loaded"
    _LOG_THRESHOLD = 10

    def __init__(self, urlSource: str, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(urlSource)
        self.stg = DeliveriesDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def laden_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.NUM_LOADED_KEY: 0, self.LAST_LOADED_TS_KEY: '2023-01-20'})

            # Вычитываем очередную пачку объектов.
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = dt.datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.origin.list_deliveries(last_loaded_ts) #(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            i = 0
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)
                self.log.info(delivery) #!! 

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.NUM_LOADED_KEY] = len(load_queue)
            ts = max([t.order_ts for t in load_queue])
            datetimeObj = dt.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = dt.datetime.strftime(datetimeObj, '%Y-%m-%d %H:%M:%S')
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Load finished on delivery with date {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
