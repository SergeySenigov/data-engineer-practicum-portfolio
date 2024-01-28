import os, sys
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
 
# import findspark
# findspark.init()
# findspark.find()

from pyspark.sql import SparkSession

#необходимая библиотека для интеграции Spark и Kafka
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        ]
    )

#создаём SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("test connect to kafka") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

print(spark)

df = (spark.read 
       .format('kafka')
       .option('kafka.bootstrap.servers', 'rc1a-bhipfo4k1kvocn6e.mdb.yandexcloud.net:9091')
       .option('kafka.security.protocol', 'SASL_SSL')
       .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
       .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"producer_consumer\" password=\"XXXXXX\";')
       .option('subscribe', 'transaction-service-input')
       .load()) 

df.printSchema()
