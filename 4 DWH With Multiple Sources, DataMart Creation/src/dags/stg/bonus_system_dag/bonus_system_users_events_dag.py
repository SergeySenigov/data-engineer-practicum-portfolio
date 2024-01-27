# import time
# import requests
# import json
import pandas as pd
# 
# from datetime import datetime, timedelta, date
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.http_hook import HttpHook
# from airflow.operators.bash_operator import BashOperator
from examples.stg.bonus_system_dag.events_loader import EventsLoader
from lib import ConnectionBuilder

import logging

import pendulum
from airflow.decorators import dag, task
from sqlalchemy import create_engine  # Для подключения к БД

# http_conn_id = HttpHook.get_connection('http_conn_id')
# api_key = http_conn_id.extra_dejson.get('api_key')
# base_url = http_conn_id.host

log = logging.getLogger(__name__)

def reci_zdravo_svim(log: logging.Logger) -> None:
    log.info("Zdravo svim!")

# def ranks_load(log: logging.Logger) -> None:
#     postgres_conn_id = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
    
#     postgres_hook_source = PostgresHook(postgres_conn_id)
#     engine = postgres_hook_source.get_sqlalchemy_engine()

# 	#conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
#     ranks_df = pd.read_sql('select * from public.ranks', con=engine)

#     print (f"length of ranks = {len(ranks_df)}")
#     print (ranks_df)
    
#     postgres_hook_dest = PostgresHook('PG_WAREHOUSE_CONNECTION')
#     engine_dest = postgres_hook_dest.get_sqlalchemy_engine()
    
#     row_count = ranks_df.to_sql("bonussystem_ranks", engine_dest, schema="stg", if_exists='append', index=False)
#     log.info(f'{row_count} rows was inserted into bonussystem_ranks')

def users_load(log: logging.Logger) -> None:
    postgres_conn_id = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
    
    postgres_hook_source = PostgresHook(postgres_conn_id)
    engine = postgres_hook_source.get_sqlalchemy_engine()

	#engine = create_engine('postgresql+psycopg2://student:student1@localhost:5432/de')

	#conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    
    users_df = pd.read_sql('select * from public.users', con=engine)

    print (f"length of users = {len(users_df)}")
    #print (ranks_df)
    
    postgres_hook_dest = PostgresHook('PG_WAREHOUSE_CONNECTION')
    engine_dest = postgres_hook_dest.get_sqlalchemy_engine()
    
    row_count = users_df.to_sql("bonussystem_users", engine_dest, schema="stg", if_exists='replace', index=False) #fail, replace, append
    log.info(f'{row_count} rows was inserted into bonussystem_users')

@dag(
    schedule_interval='1/30 * * * *',
    start_date=pendulum.datetime(2023, 1, 22, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin', 'psql'],
    is_paused_upon_creation=True
)

def psql_to_stg_bonus_system_users_events_dag():

    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task()
    def hello_task():
        reci_zdravo_svim(log)

    # @task(task_id="ranks_load_id")
    # def load_ranks():
    #     ranks_load(log)

    @task(task_id="users_load_id")
    def load_users():
        users_load(log)

    # Объявляем таск, который загружает данные.
    @task(task_id="events_load_id")
    def load_events():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = EventsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_events()  # Вызываем функцию, которая перельет данные.


    # Инициализируем объявленные таски.
    hello = hello_task()
    #ranks = load_ranks() # отключено 
    users = load_users()
    res_events = load_events()

    hello >> [users, res_events] 
    #hello >> [users, ranks, res_events] 

from_psql_to_stg_bonus_system_dagp = psql_to_stg_bonus_system_users_events_dag()

#print ("tasks :", imp_from_psql_to_stg_dag.tasks)


