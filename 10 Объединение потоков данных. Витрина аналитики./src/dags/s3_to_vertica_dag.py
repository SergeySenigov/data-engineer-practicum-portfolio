import logging
from os import listdir
from os.path import isfile, join
import csv

import pendulum
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG

import boto3
import vertica_python

from connect import VERTICA_CONFIG

DATA_PATH = '/lessons/data10/'
CURRENCIES_FILE_NAME = 'currencies_history.csv_'

log = logging.getLogger(__name__)  

def load_s3_files():
    """ Load all data files from s3 storage """
    log.info('Opening session S3')

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=Variable.get("aws_access_url"),
        aws_access_key_id=Variable.get("aws_access_key_id") ,
        aws_secret_access_key=Variable.get("aws_secret_access_key"),
    )

    # loop over all files in target bucket 'final-project' and download to DATA_PATH 
    for key in s3.list_objects(Bucket='final-project')['Contents']:
        file_name = key['Key']

        s3.download_file(
            Bucket='final-project',
            Key=file_name,
            Filename= DATA_PATH + file_name)

        # add to csv file column 'file_name' and write there file_name
        with open(DATA_PATH + file_name,'r') as csvinput:
            with open(DATA_PATH + file_name + '_', 'w') as csvoutput:
                writer = csv.writer(csvoutput, lineterminator='\n')
                reader = csv.reader(csvinput)
                all = []
                row = next(reader)
                row.append('file_name')
                all.append(row)
                for row in reader:
                    row.append(file_name)
                    all.append(row)
                writer.writerows(all)

        log.info('Downloaded file ' + file_name)

def load_to_vertica_stg():
    """ Load all data from files to Vertica staging tables """
    log.info('Start load to Vertica STG')
    
    with vertica_python.connect(**VERTICA_CONFIG) as connection:
        cur = connection.cursor()

        # get from STG transactions max loaded batch_number
        # transactions_batch_9.csv - between '_batch_' and '.'
        stmt = "select max(( SUBSTR(t.file_name, STRPOS(t.file_name, '_batch_')+7, STRPOS(t.file_name, '.') - STRPOS(t.file_name, '_batch_')-7))::int) \
                 from STV2023060652__STAGING.transactions t"
        cur.execute(stmt) 
        max_loaded_batch_number = cur.fetchall()[0][0]
        log.info("Max loaded to STG transactions batch_number: " + str(max_loaded_batch_number))

        for load_file in listdir(DATA_PATH):

            full_load_file = join(DATA_PATH, load_file)

            # process only files with new column 'file_name' - '_' at the end
            if (not isfile(full_load_file)) or full_load_file[-1] != '_':
                continue

            if load_file != CURRENCIES_FILE_NAME: 
                # get file_name batch number: transactions_batch_9.csv_ - between '_batch_' and '.'
                batch_number = load_file[load_file.find('_batch_')+7:load_file.find('.')]

                # skip files with earlier loaded batches
                if int(batch_number) < int(max_loaded_batch_number or 0):
                    # logging.info('Skipping file ' + load_file + ' due to batch_number less than max_loaded_batch_number ' + str(max_loaded_batch_number))
                    continue

            logging.info('Loading ' + load_file)

            # delete from STG this batch data
            if load_file != CURRENCIES_FILE_NAME: 
                cur.execute("delete from STV2023060652__STAGING.transactions where file_name = '" + load_file[:-1] +  "'")
                logging.info("Deleted from STV2023060652__STAGING.transactions where file_name = '" + load_file[:-1] +  "'" )
            else:
                cur.execute("truncate table STV2023060652__STAGING.currencies")
                logging.info('Table STV2023060652__STAGING.currencies cleared')

            # if file with currency data then alter insert statement: insert into __STAGING.currencies
            if load_file == CURRENCIES_FILE_NAME:
                field_list = "currency_code, currency_code_with, date_update, currency_with_div, file_name"
                params =  {'table_name': 'currencies', 'table_name_rej': 'currencies_rej' }   
            # if file with transactions data: insert into __STAGING.transactions
            else: 
                field_list = "operation_id, account_number_from, account_number_to, \
                                        currency_code, country, status, transaction_type, \
                                        amount, transaction_dt, file_name"
                params =  {'table_name': 'transactions', 'table_name_rej': 'transactions_rej' }
        
            # substutite field list and local file name in python string
            stmt = "COPY STV2023060652__STAGING.:table_name({}) \
                    FROM LOCAL '{}' \
                    DELIMITER ',' \
                    skip 1 \
                    REJECTED DATA AS TABLE STV2023060652__STAGING.:table_name_rej".format(field_list, full_load_file)

            # substitute table_name and table_name_rej by vertica_python as query parameters
            exec = cur.execute(stmt, params, buffer_size=65536)
            # get number of rows from result dataset. It is in 1st row, 1st column
            r = cur.fetchall()[0][0]
            logging.info("Rows loaded to Vertica STAGING from " + load_file + ": " + str(r))   


args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('s3_to_vertica_dag',
    default_args=args,
    schedule_interval='@daily',  # Set run daily
    start_date=pendulum.datetime(2022, 10, 1),  # Date start process
    end_date=pendulum.datetime(2022, 11, 1),  # Date end 
    catchup=True,  # Process previous periods (from start_date until today) - False (no need).
    tags=['de_project10', 's3', 'Vertica'],  
    is_paused_upon_creation=True  # Started or paused upon creation
    ) 

load_s3_files_task = PythonOperator(
        task_id='load_s3_files_task',
        python_callable=load_s3_files,
        op_kwargs={}, dag=dag)

load_to_vertica_stg_task = PythonOperator(
        task_id='load_to_vertica_stg_task',
        python_callable=load_to_vertica_stg,
        op_kwargs={}, dag=dag)

# trigger dependent DAG "vertica_stg_to_cdm_dag" and pass 3 parameters in dag_run.conf: business_dt, start_date, end_date
trigger_vertica_stg_to_cdm_dag = TriggerDagRunOperator(
        task_id="trigger_vertica_stg_to_cdm_dag",
        trigger_dag_id="vertica_stg_to_cdm_dag",  # equals the dag_id of the DAG to trigger
        conf={"business_dt": '{{ ds }}', "start_date": '{{ data_interval_start }}',  "end_date": '{{ data_interval_end }}'},
    )

load_s3_files_task >> load_to_vertica_stg_task >> trigger_vertica_stg_to_cdm_dag
