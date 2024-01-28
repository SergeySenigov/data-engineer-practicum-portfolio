# data-engineer-practicum-portfolio
Проекты специализации Data Engineer, созданные в ходе курса Yandex Practicum  

| Проект              | Цели проекта           | Используемые технологии, библиотеки, инструменты|
| :-------------------- | :--------------------- |:---------------------------|
| [User Retention DataMart](</1 Data Cleansing, User Retention DataMart Creation/README.md>)         | Проверка качества исходных данных (пропуски, повторы, форматы, некорректные записи)   <P><P>Создание витрины данных для RFM-классификации пользователей.           | SQL, Window Functions,  PostgreSQL, cloudbeaver |
| [DataModel Review and Migration to New Model](</2 DWH, DataModel Review, Migration to New Model/README.md>) | Миграция данных в отдельные логические таблицы  <P><P>Создание для новой модели данных витрины данных   | SQL, Window Functions, PostgreSQL, cloudbeaver          |
| [DataMart Review, Idempotence Support](</3 ETL Update, DataMart Review, Idempotence/README.md>)         | Модифицировать процессы в пайплайне, чтобы они соответствовали новым задачам бизнеса. Учесть backward compatibility <P><P>Создать обновленную витрину данных для исследования возвращаемости клиентов<P><P>Модифицировать ETL процесс так, чтобы поддерживалась идемпотентность         | AirFlow , SQL, PostgreSQL, cloudbeaver, bash         |
| [Data Quality Checks](</4 Check Data Quality, Check Pipeline/README.md>)     | Определить, на каких этапах процесса внедрить проверки качества данных <P><P>Разрабатать проверки и внедрить их в процесс <P><P>Создать таблицу с результатами проверок, отслеживать работу автопроверок <P><P>Составить инструкции по поддержке процессов с проверками          | AirFlow, SQL, PostgreSQL         |
| [Multiple Data Sources, Marketing DataMart](</5 DWH With Multiple Sources, DataMart Creation/README.md>)        | Усовершенствовать хранилище данных: добавить новый источник и витрину. Данные из нового источника необходимо связать с данными, которые лежат в хранилище <P><P>Реализовать витрину для расчётов с курьерами.           | AirFlow , SQL, PostgreSQL , MongoDB, MongoDB Compass, cloudbeaver, Jupyter Notebook, bash          |
| [Analytical DataBases](</6 Analytical DataBases, Vertica, DataMart Creation/README.md>)         | Расширить модель данных <P><P>Проанализировать новую информацию <P><P>Разработать для маркетологов витрину данных для оценки эффективности рекламы          | AirFlow, Yandex S3, , SQL, Vertica, cloudbeaver          |
| [Data Lake with Hadoop, HDFS](</7 Spark, Data Lake with Hadoop, HDFS/README.md>)       | Обновить, дополнить структуру данных в Data Lake <P><P>Создать новые витрины данных в HDFS, автоматизировать их обновление        | PySpark, SQL, Window Functions, Hadoop, HDFS, Airflow, Jupyter Notebook
| [Data Stream Processing](</8 Data Stream Processing>)         | Создать сервис потоковой обработки данных, расширяющий возможности онлайн приложения          | Kafka, PySpark, AirFlow, kcat, Jupyter Notebook, SQL, PostgreSQL           |
| [Yandex Cloud Services](<9 Yandex Cloud Services>)        | Реализовать на платформе Yandex Cloud сервисы, которые заполняют слои данных STG, DDS и CDM <P><P>Визуализировать данные из витрины в дашборде в Datalense          | Yandex Cloud Services, Datalense, Kubernetes, kubectl, Kafka,kcat, confluent_kafka, flask, docker, Redis         |
| [Fintech Data Processing, Analytic Datamart](</10 Fintech Data Processing, Analytic Datamart>)         |           | Yandex S3 Storage, Metabase, Vertica, vertica_python, boto3, Airflow       |
---  


## [1. Data Cleansing, User Retention DataMart Creation](</1 Data Cleansing, User Retention DataMart Creation/README.md>)

### **Цели проекта**  

Добавить практическую часть описаний

- Проверка качества исходных данных (пропуски, повторы, форматы, некорректные записи)

- Создание витрины данных для RFM-классификации пользователей. 

### **Используемые технологии и инструменты**
- SQL  
- Window Functions
- PostgreSQL  
- cloudbeaver   


## [2. DWH, DataModel Review and Migration to New Model](</2 DWH, DataModel Review, Migration to New Model/README.md>)

### **Цели проекта**  

- Миграция данных в отдельные логические таблицы  

- Собрать на новых таблицах витрину данных

### **Используемые технологии и инструменты**
- SQL  
- Window Functions
- PostgreSQL  
- cloudbeaver   

## [3. ETL Update, DataMart Review, Idempotence](</3 ETL Update, DataMart Review, Idempotence/README.md>)

### **Цели проекта**  

- Модифицировать процессы в пайплайне, чтобы они соответствовали новым задачам бизнеса. Учесть backward compatibility.

- Создать обновленную витрину данных для исследования возвращаемости клиентов.

- Модифицировать ETL процесс так, чтобы поддерживалась идемпотентность.

### **Используемые технологии и инструменты**
- AirFlow  
- ETL    
- SQL  
- PostgreSQL  
- cloudbeaver 
- bash   


## [4. Check Data Quality, Check Pipeline](</4 Check Data Quality, Check Pipeline/README.md>)

### **Цели проекта**  

- Определить, на каких этапах процесса внедрить проверки качества данных
- Разрабатать проверки и внедрить их в процесс
- Создать таблицу с результатами проверок, отслеживать работу автопроверок
- Составить инструкции по поддержке процессов с проверками

### **Используемые технологии и инструменты**
- AirFlow  
- ETL    
- SQL  
- PostgreSQL  

## [5. DWH With Multiple Sources, DataMart Creation](</5 DWH With Multiple Sources, DataMart Creation/README.md>)

### **Цели проекта**  

- Усовершенствовать хранилище данных: добавить новый источник и витрину. Данные из нового источника необходимо связать с данными, которые уже лежат в хранилище.  

- Реализовать витрину для расчётов с курьерами.  

### **Используемые технологии и инструменты**
- AirFlow  
- ETL    
- SQL  
- MongoDB
- MongoDB Compass
- PostgreSQL  
- cloudbeaver   
- Jupyter Notebook
- bash   

## [6. Analytical DataBases, Vertica](</6 Analytical DataBases, Vertica/README.md>)

## **Цели проекта**  

- Расширить модель данных
- Проанализировать новую информацию
- Разработать для маркетологов витрину данных для оценки эффективности рекламы

### **Используемые технологии и инструменты**
- AirFlow  
- ETL    
- Yandex S3
- CTL
- SQL  
- Vertica
- cloudbeaver  
- bash   

## [7. Spark, Data Lake with Hadoop, HDFS](</7 Spark, Data Lake with Hadoop, HDFS/README.md>)

## **Цели проекта**  

- Обновить, дополнить структуру данных в Data Lake (Hadoop, HDFS)
- Создать новые витрины данных в HDFS с помощью PySpark, автоматизировать их обновление

## **Используемые технологии и инструменты**

- PySpark  
- SQL  
- Window Functions  
- Hadoop  
- HDFS  
- Airflow  
- Jupyter Notebook

