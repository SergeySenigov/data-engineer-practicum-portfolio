import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_dag.ranks_loader import RankLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='1/30 * * * *',  # Задаю расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. 
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'psql'],  # Теги, использую для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def psql_to_stg_bonus_system_ranks_dag():
    # Создаю подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаю подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляю таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаю экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываю функцию, которая перельет данные.

    # Инициализирую объявленные таски.
    ranks_dict = load_ranks()

    # Далее задаю последовательность выполнения тасков.
    # Т.к. таск один, просто обозначю его здесь.
    ranks_dict  


stg_bonus_system_ranks_dag = psql_to_stg_bonus_system_ranks_dag()
