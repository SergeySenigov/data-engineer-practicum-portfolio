from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'ssenigov',
                                'start_date':datetime(2023, 4, 1),
                                'catchup': False
                                }

dag_spark = DAG(
                        dag_id = "project7_tasks",
                        default_args=default_args,
                        schedule_interval='0 0 * * *',
                        catchup=False
                        )

cities = SparkSubmitOperator(
                        task_id='cities_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["Cities"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "170",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

eventsWithUserAndCoords = SparkSubmitOperator(
                        task_id='eventsWithUserAndCoords_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["EventsWithUserAndCoords"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "170",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

eventsWithCitiesAll = SparkSubmitOperator(
                        task_id='eventsWithCitiesAll_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["EventsWithCitiesAll"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "170",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

eventsWithRegsWithCities = SparkSubmitOperator(
                        task_id='eventsWithRegsWithCities_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["EventsWithRegsWithCities"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "170",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

users = SparkSubmitOperator(
                        task_id='users_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["Users"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "200",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

userTravels = SparkSubmitOperator(
                        task_id='userTravels_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["UserTravels"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "200",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

report2 = SparkSubmitOperator(
                        task_id='report2_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["Report2"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "200",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

report3 = SparkSubmitOperator(
                        task_id='report3_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["Report3"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "200",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

report4 = SparkSubmitOperator(
                        task_id='report4_task',
                        dag=dag_spark,
                        application ='/lessons/run_geo.py' ,
                        application_args = ["Report4"],
                        conf={
                        "spark.executor.instances": "2",
                        "spark.executor.cores": "2",
                        "spark.driver.memory": "1g",
                        "spark.driver.cores": "1",
                        "spark.sql.shuffle.partitions": "200",
                        "spark.eventLog.logBlockUpdates.enabled": "True"
                        },
                        name = "GeoProject7_AirFlow"
                        )

cities >> eventsWithUserAndCoords >> eventsWithCitiesAll >> eventsWithRegsWithCities >> users >> userTravels >> [report2, report3, report4]