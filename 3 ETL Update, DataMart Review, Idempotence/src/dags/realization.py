import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BashOperator

import logging

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = 'senigov'
cohort = '8'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

logging.basicConfig(level=logging.INFO)
task_logger = logging.getLogger('airflow.task')

def f_generate_report(ti):

    logging.debug('This is a debug message')
    logging.info('This is an info message')
    logging.warning('This is a warning message')
    logging.error('This is an error message')
    logging.critical('This is a critical message')

    task_logger.info('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    task_logger.info(f'Response is {response.content}')
    task = json.loads(response.content)['task_id'] 


def f_get_report(ti):
    task_logger.info('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        status = json.loads(response.content)['status']
        task_logger.info(f'Response status is {status}')
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    task_logger.info(f'Report_id={report_id}')

def f_get_increment(date, ti):
    task_logger.info('Making request get_increment for date = ', date)
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    task_logger.info(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    task_logger.info(f'increment_id={increment_id}')


def f_upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    task_logger.info(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    task_logger.info(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df=df.drop('id', axis=1)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

   # удалим имеющуюся информацию в staging за этот день
    str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date = '{date}'" 
    engine.execute(str_del)

    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    task_logger.info(f'{row_count} rows was inserted for date {date}')

def f_upload_data_to_staging_hist(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='report_id') 
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    task_logger.info(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    task_logger.info(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df=df.drop('id', axis=1)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # удалим имеющуюся информацию в staging за этот день и ранее для исторических данных
    str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date < '{date}'" 
    engine.execute(str_del)

    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    task_logger.info(f'{row_count} rows was inserted for {date} and earlier')

args = {
    "owner": "senigov",
    'email': ['senigov@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5
}

business_dt = '{{ ds }}'

st_date=datetime.today()  

st_date = st_date - timedelta(hours=st_date.hour, minutes=st_date.minute, seconds=st_date.second, microseconds=st_date.microsecond)

with DAG(
        'sales_mart',
        default_args=args,
        description='Modified dag for sprint3 - load increments',
        catchup=True,
        start_date = st_date - timedelta(days=7),
        end_date =st_date - timedelta(days=1) + timedelta(hours=2),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=f_generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=f_get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=f_get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=f_upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_inc.sql",
        parameters={"date": {business_dt}} )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention_inc.sql",
        parameters={"date": {business_dt}} )

    print_info_task = BashOperator(
        task_id='print_info',
        bash_command='echo "Info: business_dt = ' + business_dt + '"')

    null_task = BashOperator(
        task_id='null_task',
        bash_command=''
    )

    (
            print_info_task
             >> generate_report
             >> get_report
             >> get_increment
             >> upload_user_order_inc
             >> [update_d_item_table, update_d_city_table, update_d_customer_table]
             >> null_task
             >> [update_f_sales, update_f_customer_retention]
    )


with DAG(
        'sales_mart_hist',
        default_args=args,
        description='Modified dag for sprint3 - load history',
        catchup=True,
        start_date = st_date - timedelta(days=7), 
        end_date = st_date - timedelta(days=7),
        schedule_interval= timedelta(days=1), 
) as dag_hist:

    h_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=f_generate_report)

    h_get_report = PythonOperator(
        task_id='get_report',
        python_callable=f_get_report)

    h_upload_user_order = PythonOperator(
            task_id='upload_user_order',
            python_callable=f_upload_data_to_staging_hist,
            op_kwargs={'filename': 'user_order_log.csv',
                   'date': business_dt,
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    h_update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    h_update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    h_update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    h_update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_hist.sql",
        parameters={"date": {business_dt}} )

    h_update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention_hist.sql",
        parameters={"date": {business_dt}} )

    h_print_info_task = BashOperator(
        task_id='print_info',
        bash_command='echo "Info: business_dt = ' + business_dt + '"')
 
    h_null_task = BashOperator(
        task_id='null_task',
        bash_command='' )

    (
            h_print_info_task 
            >> h_generate_report
            >> h_get_report
            >> h_upload_user_order
            >> [h_update_d_item_table, h_update_d_city_table, h_update_d_customer_table]
            >> h_null_task
            >> [h_update_f_sales, h_update_f_customer_retention]
    )