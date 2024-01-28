import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from examples.stg.order_system_dag.restaurants_saver import PgRestaurantsSaver
from examples.stg.order_system_dag.users_saver import PgUsersSaver
from examples.stg.order_system_dag.orders_saver import PgOrdersSaver

from examples.stg.order_system_dag.restaurants_loader import RestaurantsLoader
from examples.stg.order_system_dag.users_loader import UsersLoader
from examples.stg.order_system_dag.orders_loader import OrdersLoader

from examples.stg.order_system_dag.restaurants_reader import RestaurantsReader
from examples.stg.order_system_dag.users_reader import UsersReader
from examples.stg.order_system_dag.orders_reader import OrdersReader

from lib import ConnectionBuilder, MongoConnect


log = logging.getLogger(__name__)

def reci_zdravo_svim(log: logging.Logger) -> None:
    log.info("Start")

@dag(
    schedule_interval='7/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. 
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'mongo'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def mongo_to_stg_order_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task(task_id="zdravo_task_id")
    def zdravo_task():
        reci_zdravo_svim(log)

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgRestaurantsSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantsReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantsLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader_len = loader.run_copy()

        log.info(f'length of loader = {loader_len}')

    @task()
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgUsersSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = UsersReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = UsersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader_len = loader.run_copy()

        log.info(f'length of loader = {loader_len}')

    @task()
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgOrdersSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = OrdersReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrdersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader_len = loader.run_copy()

        log.info(f'length of loader = {loader_len}')

    restaurant_loader = load_restaurants()
    users_loader = load_users()
    zdravo = zdravo_task()
    orders_loader= load_orders()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    zdravo >> [users_loader, restaurant_loader, orders_loader]  # type: ignore
    


order_stg_dag = mongo_to_stg_order_system_dag()  # noqa
