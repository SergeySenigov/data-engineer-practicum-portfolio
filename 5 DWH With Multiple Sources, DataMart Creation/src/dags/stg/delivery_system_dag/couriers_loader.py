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


class CourierObj(BaseModel):
    courier_id: int = None
    name: str = None

class CouriersOriginRepository():
    LOAD_BATCH_SIZE = 30

    def __init__(self, url) -> None:
        self.url = url 

    def list_couriers(self) -> List[CourierObj]:

        objs = list()
        request_headers = {'X-Nickname': 'senigov', 'X-Cohort': 'XX', 'X-API-KEY': 'XXXXXX'}

        offset = 0

        while True:
            couriers_parameters = {'limit': self.LOAD_BATCH_SIZE,  'offset': offset, 'sort_field': 'id', 'sort_direction': 'asc'}

            rs = requests.get(self.url, headers = request_headers, params = couriers_parameters)
            print('offset =', offset, ', status_code =', rs.status_code, ', lenght is', len(json.loads(rs.content)))

            if len(json.loads(rs.content)) == 0 :
                print ('Length = 0, exiting while loop')
                break 

            if rs.status_code != 200:
                raise Exception ('Request error, status code = ' + str(rs.status_code))

            for courier in json.loads(rs.content):
                obj = CourierObj()
                obj.courier_id = courier['_id']
                obj.name = courier['name']
                objs.append(obj)

            offset += self.LOAD_BATCH_SIZE

        return objs


class CouriersDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            r = cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(courier_id, name)
                    VALUES (%(courier_id)s, %(name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        name = EXCLUDED.name;
                """,
                {
                    "courier_id": courier.courier_id,
                    "name": courier.name
                },
            )


class CouriersLader:
    WF_KEY = "couriers_http_to_stg_workflow"
    #LAST_LOADED_ID_KEY = "last_loaded_id"
    NUM_LOADED_KEY = "number_loaded"
    LAST_LOAD_TIME = "last_load_time"
    _LOG_THRESHOLD = 10

    def __init__(self, urlSource: str, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginRepository(urlSource)
        self.stg = CouriersDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def laden_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.NUM_LOADED_KEY: 0})

            # Вычитываем очередную пачку объектов.
            self.log.info(f"Total loaded before {wf_setting.workflow_settings[self.NUM_LOADED_KEY]}")

            load_queue = self.origin.list_couriers() #(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            i = 0
            for courier in load_queue:
                self.stg.insert_courier(conn, courier)
                #self.log.info(courier) #!! 

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.NUM_LOADED_KEY] = len(load_queue)
            wf_setting.workflow_settings[self.LAST_LOAD_TIME] = dt.datetime.now()
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Total loaded after {wf_setting.workflow_settings[self.NUM_LOADED_KEY]}")
