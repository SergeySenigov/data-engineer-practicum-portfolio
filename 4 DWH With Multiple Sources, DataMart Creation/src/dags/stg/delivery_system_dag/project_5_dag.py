from airflow.decorators import dag, task
import logging
from lib import ConnectionBuilder
import pendulum
from examples.stg.delivery_system_dag.couriers_loader import CouriersLader
from examples.stg.delivery_system_dag.deliveries_loader import DeliveriesLader

log = logging.getLogger(__name__)


def sagen_hallo(log: logging.Logger) -> None:
    log.info("Hallo!")

def load_info(log: logging.Logger) -> None:
    log.info("Load")

@dag(
    schedule_interval='4/15 * * * *',
    start_date=pendulum.datetime(2023, 1, 22, tz="UTC"),
    catchup=False,
    tags=['project5', 'stg', 'origin'],
    is_paused_upon_creation=True
)

def http_to_stg_delivery_system_dag():

    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def hallo_aufgabe(task_id="sagen_hello_id"):
        sagen_hallo(log)

    @task()
    def laden_couriers_aufgabe(task_id="laden_couriers_id"):
        url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
        couriers_lader = CouriersLader(url, dwh_pg_connect, log)
        couriers_lader.laden_couriers()  # Вызываем функцию, которая перельет данные.

    @task()
    def laden_deliveries_aufgabe(task_id="load_deliveries_id"):
        url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries'
        couriers_lader = DeliveriesLader(url, dwh_pg_connect, log)
        couriers_lader.laden_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    res_sagen_hallo = hallo_aufgabe()
    res_load_couriers_aufgabe = laden_couriers_aufgabe()
    res_load_deliveries_aufgabe = laden_deliveries_aufgabe()

    res_sagen_hallo >> [res_load_couriers_aufgabe, res_load_deliveries_aufgabe]

project_5_dag = http_to_stg_delivery_system_dag()
