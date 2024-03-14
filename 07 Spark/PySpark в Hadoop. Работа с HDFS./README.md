# Spark/PySpark в Hadoop. Работа с HDFS.  

| Задачи                   | Результаты |
| :-------------------- | :--------------------- |
| Расширить структуру данных в Data Lake <P><P> Создать четыре витрины данных в HDFS, автоматизировать их обновление |  | 

<!-- 
## **Цели проекта**  

- Расширить структуру данных в Data Lake
- Создать четыре витрины данных в HDFS, автоматизировать их обновление

## **Используемые технологии и инструменты**

PySpark  
SQL  
Window Functions  
Hadoop  
HDFS  
Airflow  
Jupyter Notebook  
SparkSubmitOperator  

## **Постановка задачи**

1. Определить по имеющимся координатам события и справочнику городов, в каком городе было совершено каждое событие.

2. Создать витрину с тремя полями:  
`user_id` — идентификатор пользователя.  
`act_city` — актуальный адрес. Город, из которого было отправлено последнее сообщение.  
`home_city` — домашний адрес. Последний город, в котором пользователь был дольше 27 дней.

3. Добавить в витрину атрибуты:  
`travel_count` — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.  
`travel_array` — список городов в порядке посещения.

4. Добавить в витрину атрибут `local_time` — местное время события. 

-->

## Реализация

Приложение будет содержать четыре слоя данных
Слой сырых данных (raw) - данные от поставщиков в оригинальном формате и структуре.
Исходные данные предоставляются внешней системой.

Операционный слой данных (Operational Data Store - ods) будет содержать минимально предобработанные данные
Операционный слой данных (Detail Data Store - dds) будет содержать промежуточные обработанные данные
Промышленный слой данных (Datamart Layer - dml) будет содержать готовые витрины

Проверка данных на этапе загрузки не требуется.

Пользователями являются аналитики, они будут использовать сформированные витрины данных в слое dml. 

Создание ролевой модели не требуется.

Результаты работы приложения будут сохраняться в HDFS в подкаталогах в каталоге `/user/sergeyseni/project7/dml` в виде файлов формата `CSV`, с заголовками, c разделителями ';'.

Файлы будут иметь относительно небольшой размер и будут использовататься аналитиками, поэтому такой формат подходит.
Данные будут обновляться каждые сутки в 00:00.

Так как данные о событиях будут поступать икрементно по дням, для удобства дозаписи и чтения данных в HDFS для операционного слоя сделал структуру с делением по дате события (`date`). 


Python Notebook [geo.ipynb](src/geo.ipynb) использовался для интерактивной разработки и отладки кода, после чего код был перенесен в python модуль [/src/scripts/pr7_classes.py](src/scripts/pr7_classes.py), указанный ниже.

В файле [/src/scripts/pr7_classes.py](src/scripts/pr7_classes.py) содержатся классы, созданные для удобства работы с промежуточными результатами обработки данных.
  
В файле [/src/scripts/run_geo.py](src/scripts/run_geo.py) вызов `main()` с одним параметром. Он вызывается посредством выполнения тасок AirFlow с передачей разных значений параметра в `application_args` оператора SparkSubmitOperator.

Схема выполнения DAG в AirFlow следующая:

`cities >> eventsWithUserAndCoords >> eventsWithCitiesAll >> eventsWithRegsWithCities >> users >>   userTravels >> [report2, report3, report4]`


Код DAG в AirFlow в [/src/dags/project7_dag.py](src/dags/project7_dag.py) приведен ниже

```python
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
```
