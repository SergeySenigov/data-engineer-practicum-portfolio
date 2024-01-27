import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.bash_operator import BashOperator

import boto3
import vertica_python

log = logging.getLogger(__name__)

def reci_zdravo_svim(log: logging.Logger) -> None:
    log.info("Zdravo svim!")

@dag(
    schedule_interval='0/30 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 2, 9, 6, tz="Asia/Yekaterinburg"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project6', 's3', 'Vertica'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def project6_dag():
    bucket_files_list = ['group_log.csv']

    @task(task_id="start")
    def reci_zdravo_task():
        reci_zdravo_svim(log)
        logging.info("bucket_files_list = " + str(bucket_files_list))


    @task(task_id="load_s3_files_task_id") 
    def load_s3_files_task():
        logging.info ('Start load from S3')

        AWS_ACCESS_KEY_ID = "YCAXXXXXX"
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        for file_name in bucket_files_list:
            logging.info('Downloading ' + file_name)

            s3_client.download_file(
                Bucket='sprint6',
                Key=file_name,
                Filename='/data/' + file_name
        )
        
    @task(task_id="load_to_vertica_stg_id")
    def load_to_vertica_stg_task():
        logging.info('Start load to Vertica STG')

        conn_info = {'host': Variable.get("VERTICA_HOST"), # Адрес сервера 
             'port': '5433', # Порт из инструкции,
             'user': Variable.get("VERTICA_USER"), # Полученный логин
             'password': Variable.get("VERTICA_PASSWORD"),
             'database': Variable.get("VERTICA_DB"),
             'autocommit': True
            }

        with vertica_python.connect(**conn_info) as connection:
            cur = connection.cursor()

            cur.execute("truncate table SENIGOVYANDEXRU__STAGING.group_log")

            exec = cur.execute("COPY SENIGOVYANDEXRU__STAGING.group_log(group_id, user_id, user_id_from, event, event_datetime) "
                        " FROM LOCAL '/data/group_log.csv' "
                        " DELIMITER ','"
                        " skip 1"
                        " REJECTED DATA AS TABLE SENIGOVYANDEXRU__STAGING.group_log_rej",
                        buffer_size=65536
            )
            r = cur.fetchall()
            logging.info("Rows loaded to group_log:" + str(r))


    @task(task_id="transfer_to_vertica_dds_links_task_id")
    def transfer_to_vertica_dds_links_task():
        logging.info('Start transfer to Vertica DDS links')

        conn_info = {'host': Variable.get("VERTICA_HOST"), # Адрес сервера 
             'port': '5433', # Порт из инструкции,
             'user': Variable.get("VERTICA_USER"), # Полученный логин
             'password': Variable.get("VERTICA_PASSWORD"),
             'database': Variable.get("VERTICA_DB"),
             'autocommit': True
            }

        with vertica_python.connect(**conn_info) as connection:
            cur = connection.cursor()

            cur.execute(
              " INSERT INTO SENIGOVYANDEXRU__DWH.l_user_group_activity(" 
              "     hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt,load_src) "
              "  select distinct "
              "    hash(hk_user_id, hk_group_id) as hk_l_user_group_activity, "
              "    hk_user_id, "
              "    hk_group_id, "
              "    now() as load_dt, "
              "    's3' as load_src "
              " from SENIGOVYANDEXRU__STAGING.group_log as sgl "
              " left join SENIGOVYANDEXRU__DWH.h_users hu on hu.user_id = sgl.user_id "
              " left join SENIGOVYANDEXRU__DWH.h_groups hg on hg.group_id = sgl.group_id "
              " where hash(hk_user_id, hk_group_id) not in (select hk_l_user_group_activity from SENIGOVYANDEXRU__DWH.l_user_group_activity); "
              " ; " 
            ) 
            r = cur.fetchall()
            print("Rows loaded from STG group_log to DWH l_user_group_activity:", r)

    zdravo = reci_zdravo_task()
    load_s3_files = load_s3_files_task()
    load_to_vertica_stg = load_to_vertica_stg_task()
    transfer_to_vertica_dds_links = transfer_to_vertica_dds_links_task()

    zdravo >> load_s3_files >> load_to_vertica_stg >> transfer_to_vertica_dds_links 

project6_dag = project6_dag()  
