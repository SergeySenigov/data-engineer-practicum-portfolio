# from datetime import datetime, timedelta, date
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.http_hook import HttpHook
# from airflow.operators.bash_operator import BashOperator
from examples.to_dds.users_loader import UsersLoader
from examples.to_dds.restaurants_loader import RestaurantsLoader
from examples.to_dds.timestamps_loader import TimestampsLoader
from examples.to_dds.products_loader import ProductsLoader
from examples.to_dds.orders_loader import OrdersLoader
from examples.to_dds.product_sales_loader import ProductSalesLoader
from examples.to_dds.couriers_loader import CouriersLoader
from examples.to_dds.deliveries_loader import DeliveriesLoader
from examples.to_dds.courier_deliveries_loader import CourierDeliveriesLoader
from lib import ConnectionBuilder

import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

def reci_zdravo(log: logging.Logger) -> None:
    log.info("Start")

@dag(
    schedule_interval='15/30 * * * *',
    start_date=pendulum.datetime(2023, 1, 21, tz="UTC"),
    catchup=False,
    tags=['sprint5'],
    is_paused_upon_creation=True
)

def from_stg_to_dds_dag():

    # Создаю подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="zdravo_task")
    def reci_zdravo_task():
        reci_zdravo(log)

    @task(task_id="null_task1")
    def null_task1():
        pass

    @task(task_id="null_task2")
    def null_task2():
        pass

    # # Объявляю таск, который загружает данные.
    @task(task_id="users_load_task")
    def load_users_task():

      # создаю экземпляр класса, в котором реализована логика.
        rest_loader = UsersLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываю функцию, которая перельет данные.

    @task(task_id="restaurants_load_task")
    def load_restaurants_task():
        rest_loader = RestaurantsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="timestamps_load_task")
    def load_timestamps_task():
        rest_loader = TimestampsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="products_load_task")
    def load_products_task():
        rest_loader = ProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="orders_load_task")
    def load_orders_task():
        rest_loader = OrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="sales_load_task")
    def load_sales_task():
        rest_loader = ProductSalesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="couriers_load_task")
    def load_couriers_task():
        rest_loader = CouriersLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="deliveries_load_task")
    def load_deliveries_task():
        rest_loader = DeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    @task(task_id="couriers_deliveries_load_task")
    def load_courier_deliveries_task():
        rest_loader = CourierDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    # Инициализирую объявленные таски.
    res_zdravo = reci_zdravo_task()
    res_null1 = null_task1()
    res_null2= null_task2()
    res_users = load_users_task()
    res_restaurants = load_restaurants_task()
    res_timestamps = load_timestamps_task()
    res_products = load_products_task()
    res_orders = load_orders_task()
    res_sales = load_sales_task()
    res_deliveries = load_deliveries_task()
    res_couriers = load_couriers_task()
    res_courier_deliveries = load_courier_deliveries_task()

    res_zdravo >> [res_users , res_restaurants , res_timestamps] >> res_null1 >> [res_products, res_orders] >> res_null2 >> [res_sales, res_deliveries, res_couriers] >> res_courier_deliveries


from_stg_to_dds_dag = from_stg_to_dds_dag()



