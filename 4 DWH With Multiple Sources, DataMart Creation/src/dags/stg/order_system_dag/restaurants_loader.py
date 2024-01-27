from datetime import datetime
from logging import Logger

from examples.stg import EtlSetting, EtlSettingsRepository
from examples.stg.order_system_dag.restaurants_saver import PgRestaurantsSaver
from examples.stg.order_system_dag.restaurants_reader import RestaurantsReader
from lib import PgConnect
from lib.dict_util import json2str


class RestaurantsLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_ordersystem_restaurants_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: RestaurantsReader, pg_dest: PgConnect, pg_saver: PgRestaurantsSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")
            self.log.info(f"_SESSION_LIMIT: {self._SESSION_LIMIT}")

            load_queue = self.collection_loader.get_restaurants(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from restaurants collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing restaurants.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
