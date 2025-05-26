import logging

import pendulum
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

import vertica_python

from connect import VERTICA_CONFIG

log = logging.getLogger(__name__) 

def load_to_vertica_dds(business_dt):
    """ Load data from staging tables to DDS tables on 'business_dt' """

    log.info("---------------------------------")
    log.info('Start load to Vertica DDS')
    # Passed to this DAG params from calling DAG
    log.info(f"Value of business_dt is {business_dt}")

    with vertica_python.connect(**VERTICA_CONFIG) as connection:
        cur = connection.cursor()

        # fill hub h_currencies
        stmt = "insert into STV2023060652__DWH.h_currencies(hk_currency_id, currency_code, load_dt, load_src) \
                 select distinct hash(c.currency_code), c.currency_code, now(), 'STAGING currencies " + business_dt + "' \
                 from STV2023060652__STAGING.currencies c \
                 where c.date_update = '" + business_dt +  "' \
                 and hash(c.currency_code) not in (select hk_currency_id from STV2023060652__DWH.h_currencies)"
        cur.execute(stmt) 
        log.info("Rows loaded from STG to h_currencies: " + str(cur.fetchall()[0][0]))

        # fill satellite s_currency_rates
        stmt = "insert into STV2023060652__DWH.s_currency_rates( \
                  hk_currency_id, date_update, rate, load_dt, load_src, hk_currency_rates_hashdiff) \
                select \
                 hk.hk_currency_id, \
                 c.date_update, c.currency_with_div, now(), 'STAGING currencies " + business_dt + "', \
                 hash(hk.hk_currency_id, c.date_update) \
                 from STV2023060652__STAGING.currencies c \
                 left join STV2023060652__DWH.h_currencies hk using (currency_code) \
                 where c.date_update = '" + business_dt +  "' \
                 and c.currency_code_with = 420 \
                 and hash(hk.hk_currency_id, c.date_update) not in \
                  (select hk_currency_rates_hashdiff from STV2023060652__DWH.s_currency_rates)"

        cur.execute(stmt) 
        log.info("Rows loaded from STG to s_currency_rates: " + str(cur.fetchall()[0][0]))

        # fill hub h_operations
        stmt = "insert into STV2023060652__DWH.h_operations(hk_operation_id, operation_id, load_dt, load_src) \
                 select hs, operation_id, now(), case when min(file_name) = max(file_name) then min(file_name) else min(file_name) || ', ' || max (file_name) end \
                 from ( \
                   select hash(t.operation_id) hs, t.operation_id, t.file_name \
                   from STV2023060652__STAGING.transactions t \
                   where t.transaction_dt::date = '" + business_dt +  "' \
                   and hash(t.operation_id) not in (select hk_operation_id from STV2023060652__DWH.h_operations) \
                 ) dd \
                 group by hs, operation_id " 

        cur.execute(stmt) 
        log.info("Rows loaded from STG to h_operations: " + str(cur.fetchall()[0][0]))

        # fill link between h_operations and h_currencies with additional data 
        stmt = "insert into STV2023060652__DWH.l_currency_operation(hk_l_currency_operation_id, \
                  hk_currency_id, hk_operation_id, account_number_from, account_number_to, country, \
                  status, transaction_type, amount, transaction_dt, load_dt, load_src) \
                 select distinct hash(hc.hk_currency_id, ho.hk_operation_id, t.account_number_to, t.status), \
                 hc.hk_currency_id, ho.hk_operation_id, t.account_number_from, t.account_number_to, t.country, \
                 t.status, t.transaction_type, t.amount, t.transaction_dt, now(), 'STAGING " + business_dt + "' || ' ' || t.file_name \
                 from STV2023060652__STAGING.transactions t \
                 left join STV2023060652__DWH.h_currencies hc using (currency_code) \
                 left join STV2023060652__DWH.h_operations ho using (operation_id) \
                 where t.transaction_dt::date = '" + business_dt +  "' \
                 and t.account_number_from <> '-1' \
                 and hash(hc.hk_currency_id, ho.hk_operation_id, t.account_number_to, t.status) not in (select hk_l_currency_operation_id from STV2023060652__DWH.l_currency_operation)"
        cur.execute(stmt) 
        log.info("Rows loaded from STG to l_currency_operation: " + str(cur.fetchall()[0][0]))

        # fill link between h_operations and h_currencies by dates from l_currency_operation
        stmt = "insert into STV2023060652__DWH.l_currency_operation_date(hk_l_currency_operation_date_id, \
                  hk_operation_id, hk_currency_id, transaction_date, account_number_from, country, \
                  date_status, amount, load_dt, load_src) \
                    select hs, hk_operation_id, hk_currency_id, transaction_date, \
                    account_number_from, country, status, amount, now(), 'LCO SRC: ' || load_src  \
                    from ( \
                      select hash(lco.hk_currency_id, lco.hk_operation_id, lco.transaction_dt::date) hs, \
                      lco.hk_operation_id, lco.hk_currency_id, lco.transaction_dt::date transaction_date, \
                      lco.account_number_from, lco.country, lco.status, lco.amount, lco.load_src, \
                      row_number() over (partition by lco.hk_currency_id, lco.hk_operation_id , \
                         lco.transaction_dt::date order by lco.transaction_dt desc) row_n \
                      from STV2023060652__DWH.l_currency_operation lco  \
                      where lco.transaction_dt::date = '" + business_dt +  "' \
                      and hash(lco.hk_currency_id, lco.hk_operation_id, lco.transaction_dt::date) \
                      not in (select hk_l_currency_operation_date_id from STV2023060652__DWH.l_currency_operation_date) \
                    ) dd \
                    where row_n=1 "

        cur.execute(stmt) 
        log.info("Rows loaded from DDS to l_currency_operation_date: " + str(cur.fetchall()[0][0]))

def load_to_vertica_cdm(business_dt):
    """ Load data from DDS to CDM table on 'business_dt' """

    log.info("---------------------------------")
    log.info('Start load to Vertica CDM')
    # Passed to this DAG params from calling DAG
    log.info(f"Value of business_dt is {business_dt}")

    with vertica_python.connect(**VERTICA_CONFIG) as connection:
        cur = connection.cursor()

        stmt = "delete from STV2023060652__DWH.global_metrics \
                where date_update = '" + business_dt + "'"

        cur.execute(stmt)
        r = cur.fetchall()[0][0]
        log.info("Rows deleted from CDM: " + str(r))

        stmt =  "SELECT \
          lcod.transaction_date date_update, c.currency_code currency_from, \
          sum(lcod.amount)*NVL(min(cr.rate), 1) amount_total, \
          count(distinct lcod.hk_operation_id) cnt_transactions, \
          count(distinct lcod.hk_operation_id)/count(distinct lcod.account_number_from) avg_transactions_per_account, \
          count(distinct lcod.account_number_from) cnt_accounts_make_transactions, \
          now(), 'DDS " + business_dt + "' load_src \
        FROM STV2023060652__DWH.l_currency_operation_date lcod \
        left join STV2023060652__DWH.h_currencies c using (hk_currency_id) \
        left join STV2023060652__DWH.s_currency_rates cr on cr.date_update = lcod.transaction_date \
             and cr.hk_currency_id = lcod.hk_currency_id \
        where lcod.account_number_from <> '-1' \
        and lcod.transaction_date = '" + business_dt +  "' \
        group by lcod.transaction_date, lcod.hk_currency_id, c.currency_code "

        stmt = "insert into STV2023060652__DWH.global_metrics( \
                date_update, currency_from, amount_total, cnt_transactions, \
                avg_transactions_per_account, cnt_accounts_make_transactions, load_dt, load_src)" + \
                stmt
        cur.execute(stmt) 

        # get number of rows from result dataset. It is in 1st row, 1st column
        r = cur.fetchall()[0][0]
        log.info("Rows loaded to CDM: " + str(r))
        log.info("---------------------------------")

args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('vertica_stg_to_cdm_dag',
    default_args=args,
    schedule_interval=None, # Run when triggered only
    start_date=pendulum.datetime(2022, 1, 1),  
    catchup=False,  # Process previous periods from start_date until today - False (no need).
    tags=['de_project10', 's3', 'Vertica'],  
    is_paused_upon_creation=True  
    ) 

# parameter 'business_dt' passed in dag_run.conf from calling DAG
load_to_vertica_dds_task = PythonOperator(
        task_id='load_to_vertica_dds_task',
        python_callable=load_to_vertica_dds,
        op_kwargs={'business_dt': '{{ dag_run.conf.get("business_dt") }}'}, dag=dag)

# parameter 'business_dt' passed in dag_run.conf from calling DAG
load_to_vertica_cdm_task = PythonOperator(
        task_id='load_to_vertica_cdm_task',
        python_callable=load_to_vertica_cdm,
        op_kwargs={'business_dt': '{{ dag_run.conf.get("business_dt") }}'}, dag=dag)

load_to_vertica_dds_task >> load_to_vertica_cdm_task
