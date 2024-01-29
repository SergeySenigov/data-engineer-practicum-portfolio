# data-engineer-practicum-portfolio
Проекты специализации Data Engineer, созданные в ходе курса Yandex Practicum  

| Проект              | Цели проекта           | Используемые технологии, библиотеки, инструменты| Навыки |
| :-------------------- | :--------------------- |:---------------------------|:---------------------------|
| [Data Cleansing, User Retention DataMart](</1 Data Cleansing, User Retention DataMart Creation/README.md>)         | Проверить качество исходных данных (пропуски, повторы, форматы, некорректные записи)   <P><P>Создать витрины данных для RFM-классификации пользователей           | SQL, Window Functions,  PostgreSQL, cloudbeaver |
| [DataModel Review, Migration To A New Model](</2 DWH, DataModel Review, Migration to New Model/README.md>) | Мигрировать данные в отдельные логические таблицы  <P><P>Создать витрину данных для новой модели данных   | SQL, Window Functions, PostgreSQL, cloudbeaver          |
| [DataMart Review, Idempotence Support](</3 ETL Update, DataMart Review, Idempotence/README.md>)         | Модифицировать процессы в пайплайне, чтобы они соответствовали новым задачам бизнеса, обеспечив обратную совместимость <P><P>Создать обновленную витрину данных для исследования возвращаемости клиентов<P><P>Модифицировать ETL процесс, чтобы поддерживалась идемпотентность         | AirFlow , SQL, PostgreSQL, cloudbeaver, bash         |
| [Data Quality Checks](</4 Check Data Quality, Check Pipeline/README.md>)     | Определить, на каких этапах ETL процесса внедрить проверки качества данных <P><P>Разработать и внедрить проверки в ETL процесс <P><P>Создать  витрину данных с результатами проверок <P><P>Составить инструкции по поддержке процессов с проверками          | AirFlow, SQL, PostgreSQL         |
| [Multiple Data Sources, Analytical DataMart](</5 DWH With Multiple Sources, DataMart Creation/README.md>)        | Усовершенствовать хранилище данных: добавить новый источник и витрину <P><P>Связать данные из нового источника с данными в хранилище <P><P>Реализовать витрину для расчётов с курьерами           | AirFlow , SQL, PostgreSQL , MongoDB, MongoDB Compass, cloudbeaver, Jupyter Notebook, bash          |
| [Analytical DataBases](</6 Analytical DataBases, Vertica, DataMart Creation/README.md>)         | Расширить модель данных в аналитической БД<P><P>Разработать витрину данных для оценки эффективности рекламы          | AirFlow, Yandex S3 Storage, , SQL, Vertica, cloudbeaver          |
| [Data Lake With Hadoop, HDFS](</7 Spark, Data Lake with Hadoop, HDFS/README.md>)       | Расширить структуру данных в Data Lake <P><P>Создать новые витрины данных с помощью PySpark, автоматизировать их обновление        | PySpark, SQL, Window Functions, Hadoop, HDFS, Airflow, Jupyter Notebook
| [Data Stream Processing](</8 Data Stream Processing>)         | Создать сервис потоковой обработки данных, расширяющий возможности онлайн приложения          | Kafka, PySpark, AirFlow, kcat, Jupyter Notebook, SQL, PostgreSQL, Spark Streaming          |
| [Yandex Cloud Services](<9 Yandex Cloud Services>)        | Реализовать на платформе Yandex Cloud сервисы, которые реализуют ETL процесс <P><P>Визуализировать данные из витрины в дашборде в Datalense          | Yandex Cloud Services, Datalense, Kubernetes, kubectl, Kafka,kcat, confluent_kafka, flask, docker, Redis         |
| [Fintech Data Processing, Analytical Datamart](</10 Fintech Data Processing, Analytic Datamart>)         | Для финтех-компании, предлагающей международные банковские услуги, объединить из разных источников информацию о финансовой активность пользователей   Подготовить информацию для аналитики в дашборде           | Yandex S3 Storage, Metabase, Vertica, vertica_python, boto3, Airflow       |
---  
