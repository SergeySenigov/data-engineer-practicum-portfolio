# from datetime import datetime, timedelta, date
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.http_hook import HttpHook
# from airflow.operators.bash_operator import BashOperator
from examples.to_cmd.settlement_loader import SettlementLoader
from examples.to_cmd.courier_ledger_loader import CourierLedgerLoader
from lib import ConnectionBuilder

import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

def reci_zdravo(log: logging.Logger) -> None:
    log.info("Zdravo svim!")

@dag(
    schedule_interval='20/30 * * * *',
    start_date=pendulum.datetime(2023, 1, 21, tz="UTC"),
    catchup=False,
    tags=['project5', 'cmd'],
    is_paused_upon_creation=True
)

def from_dds_to_cdm_dag():

    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="zdravo_task")
    def reci_zdravo_task():
        reci_zdravo(log)

    # # Объявляем таск, который загружает данные.
    @task(task_id="settlement_load_task")
    def load_settlement_task():
        rest_loader = SettlementLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываем функцию, которая перельет данные.

    @task(task_id="courier_ledger_load_task")
    def load_courier_ledger_task():
        rest_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    res_settlment = load_settlement_task()
    res_courier_ledger = load_courier_ledger_task()

    [res_settlment, res_courier_ledger]


from_dds_to_cdm_dag = from_dds_to_cdm_dag()



