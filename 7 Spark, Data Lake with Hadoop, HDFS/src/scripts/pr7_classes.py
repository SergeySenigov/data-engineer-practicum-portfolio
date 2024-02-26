import os, sys

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
 
import findspark
findspark.init()
findspark.find()

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType
from pyspark.storagelevel import StorageLevel

def get_session():
    return SparkSession.builder.master("yarn").getOrCreate()

def get_session_notebook():
    return SparkSession.builder.master("yarn")\
   .appName(F"GeoProject7_1004_2")\
   .config("spark.executor.instances", "2")\
   .config("spark.executor.cores", "2")\
   .config("spark.driver.memory", "1g")\
   .config("spark.driver.cores", "1")\
   .config("spark.sql.shuffle.partitions", 170)\
   .config("spark.eventLog.logBlockUpdates.enabled", True)\
   .getOrCreate()

def print_conf(spark): 
    print("app_id".ljust(44), spark.sparkContext._jsc.sc().applicationId())
    print("app_name".ljust(44), spark.sparkContext.appName)
    print("spark.sql.shuffle.partitions".ljust(44), spark.conf.get("spark.sql.shuffle.partitions"))
    print("spark.sql.adaptive.enabled".ljust(44), spark.conf.get("spark.sql.adaptive.enabled"))
    print("spark.sql.adaptive.coalescePartitions.enabled".ljust(44), spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
    print("spark.driver.cores".ljust(44), spark.conf.get("spark.driver.cores"))
    print("spark.driver.memory".ljust(44), spark.conf.get("spark.driver.memory"))
    print("spark.executor.instances".ljust(44), spark.conf.get("spark.executor.instances"))
    print("spark.executor.cores".ljust(44), spark.conf.get("spark.executor.cores"))
    print("spark.executor.memory".ljust(44), spark.conf.get("spark.executor.memory"))
    print("spark.dynamicAllocation.enabled".ljust(44), spark.conf.get("spark.dynamicAllocation.enabled"))
    print("spark.dynamicAllocation.minExecutors".ljust(44), spark.conf.get("spark.dynamicAllocation.minExecutors"))
    print("spark.dynamicAllocation.maxExecutors".ljust(44), spark.conf.get("spark.dynamicAllocation.maxExecutors"))
    print("spark.dynamicAllocation.executorIdleTimeout".ljust(44), spark.conf.get("spark.dynamicAllocation.executorIdleTimeout"))
    print("spark.dynamicAllocation.schedulerBacklogTimeout", spark.conf.get("spark.dynamicAllocation.schedulerBacklogTimeout"))
    print("spark.history.store.maxDiskUsage", spark.conf.get("spark.history.store.maxDiskUsage"))
    print("spark.eventLog.logBlockUpdates.enabled".ljust(44), spark.conf.get("spark.eventLog.logBlockUpdates.enabled")) 
    print("spark.sql.files.maxPartitionBytes".ljust(44), spark.conf.get("spark.sql.files.maxPartitionBytes")) 
    print("spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads".ljust(44), spark.conf.get("spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads")) 
    print("spark.sql.autoBroadcastJoinThreshold".ljust(44), spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))


class ReaderFromRaw():

    @classmethod
    def read(self, session, name, filter_date = None):
        base_raw_path = '/user/sergeyseni/project7/raw/' 

        if name == 'EventsRaw':
            try: 
                return session.read.parquet(base_raw_path + name).filter("date < " + "'" + filter_date + "'") 
            except Exception as E:
                print(f'ReaderFromRaw error. Caller: "{sys._getframe(1).f_code.co_name}", name: "{name}", filter_date: "{filter_date}"', E) 
                raise E

        if name in ('CitiesRaw'):
            try: 
                return session.read.parquet(base_raw_path + name)
            except Exception as E:
                print(f'ReaderFromRaw error. Caller: "{sys._getframe(1).f_code.co_name}", name: "{name}", filter_date: "{filter_date}"', E) 
                raise E
                
        raise Exception(f'ReaderFromRaw error: Not known name. Caller: "{sys._getframe(1).f_code.co_name}", name: "{name}", filter_date: "{filter_date}"') 

class ReaderCSV(object):

    @classmethod
    def read(self, session, layer, name):
        base_path = '/user/sergeyseni/project7/'

        try: 
            return session.read.csv(base_path + layer + '/' + name, sep = ";", header = True )
        except Exception as E:
            print(f'Reader error. Caller: "{sys._getframe(1).f_code.co_name}", layer: "{layer}", name: "{name}"', E) 
            raise E

class Reader(object):

    @classmethod
    def read(self, session, layer, name):
        base_path = '/user/sergeyseni/project7/'

        try: 
            return session.read.parquet(base_path + layer + '/' + name)
        except Exception as E:
            print(f'Reader error. Caller: "{sys._getframe(1).f_code.co_name}", layer: "{layer}", name: "{name}"', E) 
            raise E

class Saver(object):

    @classmethod
    def save(self, df, layer, name):
        base_path = '/user/sergeyseni/project7/'

        if name in ('EventsRaw', 'EventsWithUserAndCoords'): # убрал coalesce
            df\
            .write\
            .partitionBy(['date']) \
            .mode('overwrite') \
            .format('parquet')\
            .save(base_path + layer + '/' + name)
            return 
               
        if name in ('EventsWithCitiesPartial', 'EventsWithCitiesAll', \
                 'RegistrationsWithCities', 'EventsWithRegsWithCities'): # без coalesce, сделал repartition(F.col('date')) в calc
            df\
            .write\
            .partitionBy(['date']) \
            .mode('overwrite') \
            .format('parquet')\
            .save(base_path + layer + '/' + name)
            return 

        if name in ('Cities', 'CitiesRaw'):
            df\
            .coalesce(1)\
            .write\
            .mode('overwrite') \
            .format('parquet')\
            .save(base_path + layer + '/' + name)      
            return
        
        if name in ('Users', 'UsersNear', 'UserChannelSubscriptions', \
                    'UserCommonChannels', 'UsersCorresponded', 'UserTravelCities', 'UserTravels'):
            df\
            .write\
            .mode('overwrite') \
            .format('parquet')\
            .save(base_path + layer + '/' + name)      
            return
        
        if name in ('Report1', 'Report2', 'Report3', 'Report4'):
 
            df.repartition(1)\
            .write\
            .mode('overwrite')\
            .format('csv')\
            .option("header", "true")\
            .option("delimiter",";")\
            .save(base_path + layer + '/' + name)
            return 
        
        raise Exception(f'Saver error: Not known name. Caller: "{sys._getframe(1).f_code.co_name}", layer: "{layer}" name: "{name}"') 


class CitiesRaw(object):
    df: DataFrame = None
    layer = 'raw'
    citiesSource: DataFrame = None
    count: int = -1

    def __init__(self, session):
        self.session = session
        self.path = self.__class__.__name__

    def read(self):    
        if self.df is None:

            try: 
                self.df = ReaderFromRaw.read(self.session, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache 
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.citiesSource is None: 
            self.citiesSource = self.session.read.csv('/user/sergeyseni/data/geo/cities/actual/geo.csv', sep = ";", header = True)

        self.df = self.citiesSource # не выполняем преобразований
        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)
        self.count = self.df.count()
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, '\n', 'Layer:', self.layer, '\n', 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 


class Cities(object):
    df: DataFrame = None
    layer = 'ods'
    citiesRaw: CitiesRaw = None
    count: int = -1

    def __init__(self, session, citiesRaw: CitiesRaw):
        self.session = session
        self.path = self.__class__.__name__
        self.citiesRaw = citiesRaw

    def read(self):    
        if self.df is None:

            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.citiesRaw is None:
            raise Exception(f'citiesRaw in {self.__class__.__name__} is None') 
            
        if self.citiesRaw.df is None:
            raise Exception(f'citiesRaw.df in {self.__class__.__name__} is None') 

        if not isinstance(self.citiesRaw, CitiesRaw):
            raise Exception(f'citiesRaw in {self.__class__.__name__} has invalid type, type is {type(self.citiesRaw)}') 

        self.df = self.citiesRaw.df\
         .withColumn('city_lat', F.round(F.toRadians(F.regexp_replace('lat',r'[,]',".")), 5))\
         .withColumn('city_lon', F.round(F.toRadians(F.regexp_replace('lng',r'[,]',".")), 5)).drop('lat','lng')\
         .withColumnRenamed('id', 'city_id')

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class EventsSource(object):
    df: DataFrame = None
    layer = '-source'
    count: int = -1
    base_source_path = '/user/master/data/geo/' 

    def __init__(self, session):
        self.session = session
        self.path = 'events' 

    def read(self, filter_date): 
        if self.df is None:
            
            try: 
                self.df = self.session.read.parquet(self.base_source_path + self.path)\
                 .select('event_type', 'event.message_from', 'event.message_to', 'event.reaction_from',\
                  'event.user', 'event.subscription_channel', 'date', 'lat', 'lon', 'event.message_id', 'event.datetime')\
                 .filter("date < " + "'" + filter_date + "'") 
        
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache 
            self.df.rdd.setName(self.path)
            self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class EventsRaw(object):
    df: DataFrame = None
    layer = 'raw'
    eventsSource: EventsSource = None
    count: int = -1

    def __init__(self, session, eventsSource: EventsSource):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsSource = eventsSource

    def read(self, filter_date):    
        if self.df is None:

            try: 
                self.df = ReaderFromRaw.read(self.session, self.path, filter_date)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache 
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.eventsSource is None:
            raise Exception(f'eventsSource in {self.__class__.__name__} is None') 
            
        if self.eventsSource.df is None:
            raise Exception(f'eventsSource.df in {self.__class__.__name__} is None') 
            
        if not isinstance(self.eventsSource, EventsSource):
            raise Exception(f'eventsSource in {self.__class__.__name__} has invalid type, type is {type(self.eventsSource)}') 

        self.df = self.eventsSource.df # не выполняем преобразований
        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)
        
        self.df = self.df.coalesce(1)
                
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
      
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None')
            
class EventsWithUserAndCoords(object):
    df: DataFrame = None
    layer = 'ods'
    eventsRaw: EventsRaw = None
    count: int = -1

    def __init__(self, session, eventsRaw: EventsRaw):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsRaw = eventsRaw

    def read(self):    
        if self.df is None:

            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.eventsRaw is None:
            raise Exception(f'eventsRaw in {self.__class__.__name__} is None') 
            
        if self.eventsRaw.df is None:
            raise Exception(f'eventsRaw.df in {self.__class__.__name__} is None') 

        self.df = self.eventsRaw.df\
            .withColumn('lat', F.round(F.toRadians(F.regexp_replace('lat',r'[,]',".")), 5))\
            .withColumn('lon', F.round(F.toRadians(F.regexp_replace('lon',r'[,]',".")), 5))\
            .withColumn('user_id', F.when(F.col('event_type') == 'subscription', F.col('user'))\
                          .when(F.col('event_type') == 'reaction', F.col('reaction_from'))\
                          .when(F.col('event_type') == 'message', F.col('message_from')))\
            .selectExpr('event_type', 'user_id', 'date', 'message_id as message_id',\
                        'message_to', 'subscription_channel', 'lat', 'lon', 'datetime as event_datetime')

        self.df = self.df.cache()
        self.df.rdd.setName(self.path)
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
            
    def test_count(self):
        #self.read()
        print(self.count)
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 


class EventsWithCitiesPartial(object):

    df: DataFrame = None
    layer = 'dds'
    eventsWithUserAndCoords: EventsWithUserAndCoords
    cities: Cities = None
    count: int = -1

    def __init__(self, session, eventsWithUserAndCoords: EventsWithUserAndCoords, cities: Cities):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithUserAndCoords = eventsWithUserAndCoords
        self.cities = cities

    def read(self):
        if self.df is None:

            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.eventsWithUserAndCoords is None:
            raise Exception(f'eventsWithUserAndCoords in {self.__class__.__name__} is None') 
            
        if self.eventsWithUserAndCoords.df is None:
            raise Exception(f'eventsWithUserAndCoords.df in {self.__class__.__name__} is None') 

        if self.cities is None:
            raise Exception(f'cities in {self.__class__.__name__} is None') 
            
        if self.cities.df is None:
            self.cities.read()

        if not isinstance(self.eventsWithUserAndCoords, EventsWithUserAndCoords):
            raise Exception(f'eventsWithUserAndCoords in {self.__class__.__name__} is of wrong type: {type(self.eventsWithUserAndCoords)}') 

        if not isinstance(self.cities, Cities):
            raise Exception(f'cities in {self.__class__.__name__} is of wrong type: {type(self.cities)}') 

        self.df = self.eventsWithUserAndCoords.df.drop('city','id', 'evt_id', 'city_id')\
          .withColumn('evt_id', F.monotonically_increasing_id())
        
        ## broadcast!
        self.df = self.df.crossJoin(F.broadcast(self.cities.df))\
         .withColumn('dist', F.acos(F.sin(F.col('city_lat'))*F.sin(F.col('lat')) + F.cos(F.col('city_lat'))*F.cos(F.col('lat'))*F.cos(F.col('city_lon')-F.col('lon')))*F.lit(6371))
    
        w = Window().partitionBy('evt_id').orderBy(F.asc_nulls_last('dist'))
    
        self.df = self.df.withColumn("row", F.row_number().over(w)).filter(F.col('row')==1)
    
        self.df = self.df\
         .withColumn('city_id', F.when(~F.isnull('dist'), F.col('city_id')))\
         .withColumn('city', F.when(~F.isnull('dist'), F.col('city')))\
         .drop('row', 'city_lat', 'city_lon')   

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)

        self.df = self.df.repartition(F.col('date'))
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()


    def test_missing_cities(self):
        self.read()
        # у скольки событий по типам пустой город

        res = self.df.groupBy('event_type').agg(\
                F.count_if('city_id = 0').alias('city_id_null'),
                F.count_if('city_id is null 0').alias('city_id_isnull'),
                F.count_if('city is null').alias('city_isnull'),
                F.count_if('city = "N/A"').alias('city_is_NA'),
                F.count('*').alias('total')).cache()

        res.collect()
        res.print()

    def test_count(self):
        print(self.count)
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class EventsWithCitiesAll(object):

    df: DataFrame = None
    layer = 'dds'
    eventsWithCitiesPartial: EventsWithCitiesPartial = None
    count: int = -1

    def __init__(self, session, eventsWithCitiesPartial: EventsWithCitiesPartial):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithCitiesPartial = eventsWithCitiesPartial

    def read(self):    
        if self.df is None:
            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.eventsWithCitiesPartial is None:
            raise Exception(f'eventsWithCitiesPartial in {self.__class__.__name__} is None') 
            
        if self.eventsWithCitiesPartial.df is None:
            self.eventsWithCitiesPartial.read()
            #raise Exception(f'eventsWithCitiesPartial.df in {self.__class__.__name__} is None') 

    
        w = Window().partitionBy('user_id').orderBy(F.when(F.col('event_type') == 'message', 1).otherwise(0).desc(), F.desc('date'))

        self.df = self.eventsWithCitiesPartial.df\
         .withColumn('added', F.when( F.isnull('city_id') & (F.first('event_type', ignorenulls = True).over(w) == 'message') & ~F.isnull( F.first('city_id', ignorenulls = True).over(w)) , 1).otherwise(0))\
         .withColumn('city_id', F.when( F.isnull('city_id') & (F.first('event_type', ignorenulls = True).over(w) == 'message') , F.first('city_id', ignorenulls = True).over(w) ).otherwise(F.col('city_id')))\
         .withColumn('city', F.when( F.isnull('city') & (F.first('event_type', ignorenulls = True).over(w) == 'message'), F.first('city', ignorenulls = True).over(w) ).otherwise(F.col('city')))

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)

        self.df = self.df.repartition(F.col('date'))
        
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()

    def test_where_added(self):
        self.read()
        self.df.where("added = 1").orderBy("user_id").show()
    
    def test_count(self):
        print(self.count)
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 
            
    def test_stat(self):
        res = self.df.groupBy('event_type', 'added').agg(\
                F.count(F.when(F.isnull('user_id'), F.lit(1))).alias('user_null_cnt'),
                F.count(F.when(F.isnull('city'), F.lit(1))).alias('city_is_null'),
                F.count(F.when(F.isnull('city_id'), F.lit(1))).alias('city_id_is_null'),
                F.count(F.when(F.isnull('dist'), F.lit(1))).alias('dist_is_null'),
                F.count(F.when(F.isnull('message_id'), F.lit(1))).alias('message_id_is_null'),
                F.count('*').alias('total')).cache()
        res.show(30)
        
    def test_stat2(self): 
        res = self.df.agg(F.countDistinct('user_id'))
        res.show()


class RegistrationsWithCities(object):

    df: DataFrame = None
    layer = 'dds'
    eventsWithCitiesAll: EventsWithCitiesAll = None
    count: int = -1

    def __init__(self, session, eventsWithCitiesAll: EventsWithCitiesAll):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithCitiesAll = eventsWithCitiesAll

    def read(self):    
        if self.df is None:
            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.eventsWithCitiesAll is None:
            raise Exception(f'eventsWithCitiesAll in {self.__class__.__name__} is None') 
            
        if self.eventsWithCitiesAll.df is None:
            raise Exception(f'eventsWithCitiesAll.df in {self.__class__.__name__} is None') 
    
        # событие регистрации - взять первое событие пользователя, добавить условие - где определен город
        w = Window().partitionBy('user_id').orderBy(F.col('date').asc()) #по возрастанию

        self.df = self.eventsWithCitiesAll.df.where("city is not null") # добавить условие - где определен город
        
        self.df = self.df.withColumn("row", F.row_number().over(w)).filter(F.col('row')==1)\
          .withColumn('event_type', F.lit('user_registration')).drop('row')
        
        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)

        self.df = self.df.repartition(F.col('date'))
        
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()

    def test_where_added(self):
        self.read()
        self.df.where("added = 1").orderBy("user_id").show()
    
    def test_count(self):
        print(self.count)
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 
            
    def test_stat(self):
        res = self.df.groupBy('event_type', 'added').agg(\
                F.count(F.when(F.isnull('user_id'), F.lit(1))).alias('user_null_cnt'),
                F.count(F.when(F.isnull('city'), F.lit(1))).alias('city_is_null'),
                F.count(F.when(F.isnull('city_id'), F.lit(1))).alias('city_id_is_null'),
                F.count(F.when(F.isnull('dist'), F.lit(1))).alias('dist_is_null'),
                F.count(F.when(F.isnull('message_id'), F.lit(1))).alias('message_id_is_null'),
                F.count('*').alias('total')).cache()
        res.show(30)
        
    def test_stat2(self): 
        res = self.df.agg(F.countDistinct('user_id'))
        res.show()


class EventsWithRegsWithCities(object):

    df: DataFrame = None
    layer = 'dds'
    eventsWithCitiesAll: EventsWithCitiesAll = None
    registrationsWithCities: RegistrationsWithCities = None
    count: int = -1

    def __init__(self, session, eventsWithCitiesAll: EventsWithCitiesAll\
                              , registrationsWithCities: RegistrationsWithCities):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithCitiesAll = eventsWithCitiesAll
        self.registrationsWithCities = registrationsWithCities

    def read(self):    
        if self.df is None:
            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.eventsWithCitiesAll is None:
            raise Exception(f'eventsWithCitiesAll in {self.__class__.__name__} is None') 
            
        if self.eventsWithCitiesAll.df is None:
            #self.eventsWithCitiesAll.read()
            raise Exception(f'eventsWithCitiesAll.df in {self.__class__.__name__} is None') 

        if self.registrationsWithCities is None:
            raise Exception(f'registrationsWithCities in {self.__class__.__name__} is None') 
            
        if self.registrationsWithCities.df is None:
            #self.registrationsWithCities.read()
            raise Exception(f'registrationsWithCities.df in {self.__class__.__name__} is None') 

        # объединить оба датасета: событий и регистраций

        self.df = self.eventsWithCitiesAll.df.unionByName(self.registrationsWithCities.df)
        
        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)    
        
        self.df = self.df.repartition(F.col('date'))
        
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()

    def test_count(self):
        print(self.count)
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class Users(object):
    df: DataFrame = None
    layer = 'dds'
    eventsWithCitiesAll: EventsWithCitiesAll = None
    count: int = -1

    def __init__(self, session, eventsWithCitiesAll: EventsWithCitiesAll):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithCitiesAll = eventsWithCitiesAll

    def read(self):    
        if self.df is None:
            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.eventsWithCitiesAll is None:
            raise Exception(f'eventsWithCitiesAll in {self.__class__.__name__} is None') 
            
        if self.eventsWithCitiesAll.df is None:
            #self.eventsWithCitiesPartial.read()
            raise Exception(f'eventsWithCitiesAll.df in {self.__class__.__name__} is None') 

        #нужно из списка событий получить список пользователей
    
        self.df = self.eventsWithCitiesAll.df
        
            
        # root
        #  |-- event_type: string (nullable = true)
        #  |-- user_id: string (nullable = true)
        #  |-- message_id: long (nullable = true)
        #  |-- message_to: long (nullable = true)
        #  |-- subscription_channel: long (nullable = true)
        #  |-- lat: double (nullable = true)
        #  |-- lon: double (nullable = true)
        #  |-- evt_id: long (nullable = true)
        #  |-- city_id: string (nullable = true)
        #  |-- city: string (nullable = true)
        #  |-- dist: double (nullable = true)
        #  |-- date: date (nullable = true)
    
        
        # добавляю колонки "actual_city" по последнему _сообщению_ пользователя
        # это первая строка из окна событий по user_id с сортировкой по типу события "message" и дате по убыванию
        # если в первой строке событие не "message", значит у пользователя вообще нет сообщений
        # тогда не заполняем, там может быть другой тип события
        w1 = Window().partitionBy('user_id').orderBy(F.when(F.col('event_type') == 'message', 1).otherwise(0).desc(), F.desc('date'))

        self.df = self.df\
         .withColumn('actual_city_id', F.when( F.first('event_type', ignorenulls = True).over(w1) == 'message', F.first('city_id').over(w1) ))\
         .withColumn('actual_city',    F.when( F.first('event_type', ignorenulls = True).over(w1) == 'message', F.first('city').over(w1) ))\
         .withColumn('actual_lat',     F.when( F.first('event_type', ignorenulls = True).over(w1) == 'message', F.first('lat').over(w1) ))\
         .withColumn('actual_lon',    F.when( F.first('event_type', ignorenulls = True).over(w1) == 'message', F.first('lon').over(w1) ))

        # для проверки на полноту информации создал поле last_message_id
        self.df = self.df\
         .withColumn('last_message_id', F.when( F.first('event_type', ignorenulls = True).over(w1) == 'message', F.first('message_id').over(w1) ))
        
        # добавил колонки "last_event_city" по последнему сообытию пользователя
        # это первая строка из окна событий по user_id с сортировкой по дате по убыванию
        # если у события нет города, то останется пустым
        w2 = Window().partitionBy('user_id').orderBy(F.col('date').desc())

        self.df = self.df\
         .withColumn('last_event_city_id', F.first('city_id').over(w2) )\
         .withColumn('last_event_city',    F.first('city').over(w2) )\
         .withColumn('last_event_datetime', F.first('event_datetime').over(w2) )
        
        # оставим только нужные колонки для Users и оставим только уникальные строки 
        self.df = self.df.select('user_id', 'actual_city_id', 'actual_city', \
                                 'last_event_city_id', 'last_event_city', 'last_message_id', \
                                 'actual_lat', 'actual_lon', 'last_event_datetime').distinct()
        
        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)
        
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 
            
    def test_users_with_messages_cities_cnt(self):
        res = self.df.agg(
              F.count(F.col('last_message_id')).alias('user_with_messages_cnt'),
              F.count(F.col('actual_city_id')).alias('user_with_actual_city_cnt'),
              F.count(F.col('last_event_city_id')).alias('user_with_last_event_city_cnt'),
              F.count('*').alias('user_total_cnt')
        )
        res.show()
            
    def test_dist_by_events(self):
        res = self.df.groupBy('actual_city_id', 'last_event_city_id').agg(\
                F.count('*').alias('total')).cache()
        res.show(30)
        
    def test_dist_by_cities(self):
        res = self.df.groupBy('actual_city_id', 'actual_city').agg(\
                F.count('*').alias('total')).cache()
        res.show(30)
        
    def test_actual_city_count(self): 
        res = self.df.agg(F.countDistinct('actual_city_id'))
        res.show()

class UserChannelSubscriptions(object):

    df: DataFrame = None
    layer = 'dds'
    eventsWithUserAndCoords: object
    count: int = -1

    def __init__(self, session, eventsWithUserAndCoords: EventsWithUserAndCoords):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithUserAndCoords = eventsWithUserAndCoords

    def read(self):
        if self.df is None:

            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool, sampleRate: float):

        if self.eventsWithUserAndCoords is None:
            raise Exception(f'eventsWithUserAndCoords in {self.__class__.__name__} is None') 
            
        if self.eventsWithUserAndCoords.df is None:
            raise Exception(f'eventsWithUserAndCoords.df in {self.__class__.__name__} is None')             

        if not isinstance(self.eventsWithUserAndCoords, EventsWithUserAndCoords):
            raise Exception(f'eventsWithUserAndCoords in {self.__class__.__name__} is of wrong type: {type(self.eventsWithUserAndCoords)}') 
    
        # взять события "подписка" на канал (не пустой subscription_channel, так как еще есть подписка на пользователя) 
        # и оставить уникальные записи
        self.df = self.eventsWithUserAndCoords.df
        
        self.df = self.df.filter(F.col('event_type') == 'subscription')\
          .filter(~F.isnull('subscription_channel'))
    
        self.df = self.df.select('user_id', 'subscription_channel').distinct()

        self.df = self.df.sample(sampleRate)

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()

        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class UserCommonChannels(object):

    df: DataFrame = None
    layer = 'dds'
    userChannelSubscriptionsLeft: object
    userChannelSubscriptionsRight: object
    count: int = -1

    def __init__(self, session, userChannelSubscriptionsLeft: UserChannelSubscriptions,
                     userChannelSubscriptionsRight: UserChannelSubscriptions):
        self.session = session
        self.path = self.__class__.__name__
        self.userChannelSubscriptionsLeft = userChannelSubscriptionsLeft
        self.userChannelSubscriptionsRight = userChannelSubscriptionsRight

    def read(self):
        if self.df is None:

            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.userChannelSubscriptionsLeft is None:
            raise Exception(f'userChannelSubscriptionsLeft in {self.__class__.__name__} is None') 
            
        if self.userChannelSubscriptionsLeft.df is None:
            raise Exception(f'userChannelSubscriptionsLeft.df in {self.__class__.__name__} is None')             

        if self.userChannelSubscriptionsRight is None:
            raise Exception(f'userChannelSubscriptionsRight in {self.__class__.__name__} is None') 
            
        if self.userChannelSubscriptionsRight.df is None:
            raise Exception(f'userChannelSubscriptionsRight.df in {self.__class__.__name__} is None')             

        if not isinstance(self.userChannelSubscriptionsLeft, UserChannelSubscriptions):
            raise Exception(f'userChannelSubscriptionsLeft in {self.__class__.__name__} is of wrong type: {type(self.eventsWithUserAndCoords)}') 

        if not isinstance(self.userChannelSubscriptionsRight, UserChannelSubscriptions):
            raise Exception(f'userChannelSubscriptionsRight in {self.__class__.__name__} is of wrong type: {type(self.eventsWithUserAndCoords)}') 

        # сделать объединение двух датасетов по subscription_channel 
        # и оставить уникальные строки
        self.df = self.userChannelSubscriptionsLeft.df.withColumnRenamed('user_id', 'user_left').join(\
                          self.userChannelSubscriptionsRight.df.withColumnRenamed('user_id', 'user_right'),\
                          'subscription_channel', 'inner')
    
        self.df = self.df.select('user_left', 'user_right', 'subscription_channel').distinct()

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()

        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 


class UsersCorresponded(object):

    df: DataFrame = None
    layer = 'dds'
    eventsWithUserAndCoords: EventsWithUserAndCoords
    count: int = -1

    def __init__(self, session, eventsWithUserAndCoords: EventsWithUserAndCoords):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithUserAndCoords = eventsWithUserAndCoords

    def read(self):
        if self.df is None:

            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.eventsWithUserAndCoords is None:
            raise Exception(f'eventsWithUserAndCoords in {self.__class__.__name__} is None') 
            
        if self.eventsWithUserAndCoords.df is None:
            raise Exception(f'eventsWithUserAndCoords.df in {self.__class__.__name__} is None')             

        if not isinstance(self.eventsWithUserAndCoords, EventsWithUserAndCoords):
            raise Exception(f'eventsWithUserAndCoords in {self.__class__.__name__} is of wrong type: {type(self.eventsWithUserAndCoords)}') 

        # выбрать, где событие типа message и заполнен message_to
        # и оставить уникальные строки
        self.df = self.eventsWithUserAndCoords.df.where(" event_type = 'message' and message_to is not null ") 

        self.df = self.df.withColumnRenamed('user_id', 'user_left')\
                         .withColumn('user_right', F.col('message_to').cast(StringType()))\
                         .select('user_left', 'user_right').distinct()
        
        #упорядочить пары - сделать слева меньшее значение
        self.df = self.df.withColumn('temp_left', F.col('user_left'))
        self.df = self.df.withColumn('user_left', F.when(F.col('user_left')>F.col('user_right'), F.col('user_right')).otherwise(F.col('user_left')))
        self.df = self.df.withColumn('user_right', F.when(F.col('temp_left')>F.col('user_right'), F.col('temp_left')).otherwise(F.col('user_right')))
        self.df = self.df.drop('temp_left')
        self.df = self.df.select('user_left', 'user_right').distinct()

        self.df = self.df.cache() ##!! cache
        #self.df.rdd.setName(self.path)
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()

        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            #print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class UsersNear(object):

    df: DataFrame = None
    layer = 'dds'
    usersLeft: Users
    usersRight: Users
    count: int = -1

    def __init__(self, session, usersLeft: Users, usersRight: Users):
        self.session = session
        self.path = self.__class__.__name__
        self.usersLeft = usersLeft
        self.usersRight = usersRight

    def read(self):
        if self.df is None:

            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):

        if self.usersLeft is None:
            raise Exception(f'usersLeft in {self.__class__.__name__} is None') 
            
        if self.usersLeft.df is None:
            raise Exception(f'usersLeft.df in {self.__class__.__name__} is None')             

        if self.usersRight is None:
            raise Exception(f'usersRight in {self.__class__.__name__} is None') 
            
        if self.usersRight.df is None:
            raise Exception(f'usersRight.df in {self.__class__.__name__} is None')             

        if not isinstance(self.usersLeft, Users):
            raise Exception(f'usersLeft in {self.__class__.__name__} is of wrong type: {type(self.usersLeft)}') 

        if not isinstance(self.usersRight, Users):
            raise Exception(f'usersRight in {self.__class__.__name__} is of wrong type: {type(self.usersRight)}') 

        # посчитать расстояние по координатам для каждой пары пользователей 
        # и оставить уникальные строки
       
        # root
        #  |-- user_id: string (nullable = true)
        #  |-- actual_city_id: string (nullable = true)
        #  |-- actual_city: string (nullable = true)
        #  |-- last_event_city_id: string (nullable = true)
        #  |-- last_event_city: string (nullable = true)
        #  |-- last_message_id: long (nullable = true)
        #  |-- actual_lat: double (nullable = true)
        #  |-- actual_lon: double (nullable = true)
        #  |-- last_event_datetime
        
        self.usersLeft.df = self.usersLeft.df.drop('last_event_city_id', 'last_event_city', 'last_message_id', 'last_event_datetime')
        self.usersRight.df = self.usersRight.df.drop('last_event_city_id', 'last_event_city', 'last_message_id', 'last_event_datetime')

        # root
        #  |-- user_id: string (nullable = true)
        #  |-- actual_city_id: string (nullable = true)
        #  |-- actual_city: string (nullable = true)
        #  |-- actual_lat: double (nullable = true)
        #  |-- actual_lon: double (nullable = true)
        
        # уберем тех, где нельзя посчитать расстояние
        self.usersLeft.df = self.usersLeft.df.filter(F.col('actual_lat').isNotNull())
        self.usersRight.df = self.usersRight.df.filter(F.col('actual_lat').isNotNull())
        
        self.usersLeft.df = self.usersLeft.df\
           .withColumnRenamed('actual_lat', 'left_lat')\
           .withColumnRenamed('actual_lon', 'left_lon')\
           .withColumnRenamed('user_id', 'user_left')\
           .withColumnRenamed('actual_city_id', 'city_id_left')\
           .withColumnRenamed('actual_city', 'city_left')
        
        self.usersRight.df = self.usersRight.df\
           .withColumnRenamed('user_id', 'user_right')\
           .withColumnRenamed('actual_city_id', 'city_id_right')\
           .withColumnRenamed('actual_city', 'city_right')
        
        # сделаю repartition обоих входных датасетов, чтобы умножение прошло на многих исполнителях
        
        self.usersLeft.df = self.usersLeft.df.repartition(100)
        self.usersRight.df = self.usersRight.df.repartition(100)
        
        # вычислим расстояние между всеми парами пользователей
        self.df = self.usersLeft.df.crossJoin(self.usersRight.df)
        
        # оставим только уникальные пары
        self.df = self.df.where("user_left < user_right")
        
        self.df = self.df.withColumn('dist', F.acos(F.sin(F.col('left_lat'))*F.sin(F.col('actual_lat')) + F.cos(F.col('left_lat'))*F.cos(F.col('actual_lat'))*F.cos(F.col('left_lon')-F.col('actual_lon')))*F.lit(6371))
    
        # расстояние не больше 1 км
        self.df = self.df.filter(F.col('dist') <= 1) 

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)
        
        self.df = self.df.repartition(1)
        
        if save:  
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()

        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class UserTravelCities(object):

    df: DataFrame = None
    layer = 'dds'
    eventsWithCitiesAll: EventsWithCitiesAll = None
    count: int = -1

    def __init__(self, session, eventsWithCitiesAll: EventsWithCitiesAll):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithCitiesAll = eventsWithCitiesAll

    def read(self):    
        if self.df is None:
            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.eventsWithCitiesAll is None:
            raise Exception(f'eventsWithCitiesAll in {self.__class__.__name__} is None') 
            
        if self.eventsWithCitiesAll.df is None:
            #self.eventsWithCitiesAll.read()
            raise Exception(f'eventsWithCitiesAll.df in {self.__class__.__name__} is None') 
    
        # root
        #  |-- event_type: string (nullable = true)
        #  |-- user_id: string (nullable = true)
        #  |-- message_id: long (nullable = true)
        #  |-- message_to: long (nullable = true)
        #  |-- subscription_channel: long (nullable = true)
        #  |-- lat: double (nullable = true)
        #  |-- lon: double (nullable = true)
        #  |-- evt_id: long (nullable = true)
        #  |-- city_id: string (nullable = true)
        #  |-- city: string (nullable = true)
        #  |-- dist: double (nullable = true)
        #  |-- date: date (nullable = true)

        self.df = self.eventsWithCitiesAll.df

        self.df = self.df.where("city is not null")
        # ставим признаки начала и конца пребывания с помощью lead и lag сравнивая с предыдущим и следующим городом
        w = Window().partitionBy('user_id').orderBy(F.col('date').asc())

        self.df = self.df.withColumn('start_flag', F.when( (F.col('city') != F.lag('city').over(w) ) | F.isnull(F.lag('city').over(w)), 1))
        self.df = self.df.withColumn('end_flag', F.when( (F.col('city') != F.lead('city').over(w) ) | F.isnull(F.lead('city').over(w)), 1))

        # в каждой строке делаем подсчет start_flag, таким образом получаем группы строк с одинаковым city_stay_id для каждого пребывания
        self.df = self.df.withColumn('stay_id', F.count('start_flag').over(w))
        # когда заполняется поле start_flag, то не указан otherwise, 
        # соответственно там будет "1" - для строки начала каждого пребывания и null для всех остальных строк пребывания
        # таким образом count и sum сработают одинаково, обе функции null посчитают как 0,
        # и для каждой непрерывной последовательности пребывания это значение count (или sum) будет одинаковым

        w2 = Window().partitionBy('user_id', 'stay_id')
        
        # в образованной группе присваиваем всем одинаковые значения начала и конца пребывания
        self.df = self.df.withColumn('city_stay_start', F.min('date').over(w2))
        self.df = self.df.withColumn('city_stay_end', F.max('date').over(w2))

        # считаем длительность пребывания
        self.df = self.df.withColumn("city_stay_len", F.datediff('city_stay_end', 'city_stay_start')+1)

        self.df = self.df.select('user_id', 'city_id', 'city', 'city_stay_start', 'city_stay_end', 'city_stay_len').distinct()

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)

        self.df = self.df.repartition(5)
        
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class UserTravels(object):

    df: DataFrame = None
    layer = 'dds'
    userTravelCities: UserTravelCities = None
    count: int = -1

    def __init__(self, session, userTravelCities: UserTravelCities):
        self.session = session
        self.path = self.__class__.__name__
        self.userTravelCities = userTravelCities

    def read(self):    
        if self.df is None:
            try: 
                self.df = Reader.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.userTravelCities is None:
            raise Exception(f'userTravelCities in {self.__class__.__name__} is None') 
            
        if self.userTravelCities.df is None:
            #self.eventsWithCitiesAll.read()
            raise Exception(f'userTravelCities.df in {self.__class__.__name__} is None') 
    
        # root
        #  |-- user_id: string (nullable = true)
        #  |-- city_id: string (nullable = true)
        #  |-- city: string (nullable = true)
        #  |-- city_stay_start: date (nullable = true)
        #  |-- city_stay_end: date (nullable = true)
        #  |-- city_stay_len: integer (nullable = true)

        self.df = self.userTravelCities.df

        self.df = self.df.withColumn('long_stay_flag', F.when(F.col('city_stay_len') > 27, 1).otherwise(0))

        # делаем окно, где первой строкой - город, где пребывание > 27 дней и наибольшей датой, если такой есть
        w = Window.partitionBy('user_id').orderBy(F.desc('long_stay_flag'), F.desc('city_stay_start'))

        self.df = self.df.withColumn("home_city", F.when(F.first('long_stay_flag').over(w) == 1, F.first('city').over(w))   )\
           .withColumn("home_city_id", F.when(F.first('long_stay_flag').over(w) == 1, F.first('city_id').over(w))   ) 
        
        self.df = self.df.groupby("user_id").agg(
            # сортировка же идет не по названиям городов, а по структуре с полем даты в начале 
            # F.sort_array(F.collect_list(F.struct("city_stay_start", "city"))).alias("tuples_array")
            # а далее из поля tuples_array берем города
            # .withColumn("travel_array", F.col("tuples_array.city")
           F.sort_array(F.collect_list(F.struct("city_stay_start", "city"))).alias("tuples_array"), \
           F.count('user_id').alias('travel_count'), \
           F.min('home_city').alias('home_city'), \
           F.min('home_city_id').alias('home_city_id'), \
           )\
           .withColumn("travel_array", F.col("tuples_array.city"))
          
  
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 


class Report2(object):

    df: DataFrame = None
    layer = 'dml'
    users: Users = None
    userTravels: UserTravels = None
    count: int = -1

    def __init__(self, session, users: Users, userTravels: UserTravels):
        self.session = session
        self.path = self.__class__.__name__
        self.users = users
        self.userTravels = userTravels

    def read(self):    
        if self.df is None:
            try: 
                self.df = ReaderCSV.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.users is None:
            raise Exception(f'users in {self.__class__.__name__} is None') 
            
        if self.users.df is None:
            #self.eventsWithCitiesAll.read()
            raise Exception(f'users.df in {self.__class__.__name__} is None') 

        if self.userTravels is None:
            raise Exception(f'userTravels in {self.__class__.__name__} is None') 
            
        if self.userTravels.df is None:
            #self.eventsWithCitiesAll.read()
            raise Exception(f'userTravels.df in {self.__class__.__name__} is None')    


        # связать два датасета по user_id
        # из users берем actual_city, last_event_city
        # из userTravels home_city, travel_count, travel_array
        self.df = self.users.df.join(self.userTravels.df, 'user_id', 'outer')

        self.df = self.df.withColumn("TIME",F.col("last_event_datetime").cast("Timestamp"))

        self.df = self.df.withColumn('local_time', \
                    F.when( F.col('last_event_city').isin('Sydney', 'Melbourne', 'Brisbane', 'Perth', \
                         'Adelaide', 'Canberra', 'Hobart', 'Darwin') \
                    , F.from_utc_timestamp(F.col('TIME'), F.concat(F.lit('Australia/'), F.col('last_event_city'))))\
                     .otherwise(None))
                
        self.df = self.df.withColumn('travel_array', F.concat_ws(',', F.col('travel_array')))
                
        self.df = self.df.select('user_id', 'actual_city', 'home_city', 'travel_count', 'travel_array', 'local_time')
        self.df = self.df.withColumnRenamed('actual_city', 'act_city')
        
    
        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)    
                
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class Report3(object):

    df: DataFrame = None
    layer = 'dml'
    eventsWithRegsWithCities: EventsWithRegsWithCities = None
    count: int = -1

    def __init__(self, session, eventsWithRegsWithCities: EventsWithRegsWithCities):
        self.session = session
        self.path = self.__class__.__name__
        self.eventsWithRegsWithCities = eventsWithRegsWithCities

    def read(self):    
        if self.df is None:
            try: 
                self.df = ReaderCSV.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.eventsWithRegsWithCities is None:
            raise Exception(f'eventsWithRegsWithCities in {self.__class__.__name__} is None') 
            
        if self.eventsWithRegsWithCities.df is None:
            #self.eventsWithCitiesAll.read()
            raise Exception(f'eventsWithRegsWithCities.df in {self.__class__.__name__} is None') 
            
        self.df = self.eventsWithRegsWithCities.df\
          .withColumn('month', F.trunc(F.col('date'), 'Month'))\
          .withColumn('week', F.trunc(F.col('date'), 'Week'))

        w = Window().partitionBy('month', 'city_id')


        self.df = self.df.groupBy('month', 'week', 'city_id').agg(\
                    F.sum(F.when(F.col('event_type') == 'message', F.lit(1))).alias('week_message'),\
                    F.sum(F.when(F.col('event_type') == 'reaction', F.lit(1))).alias('week_reaction'),\
                    F.sum(F.when(F.col('event_type') == 'subscription', F.lit(1))).alias('week_subscription'),\
                    F.sum(F.when(F.col('event_type') == 'user_registration', F.lit(1))).alias('week_user'))\
              .withColumn("month_message", F.sum('week_message').over(w))\
              .withColumn("month_reaction", F.sum('week_reaction').over(w))\
              .withColumn("month_subscription", F.sum('week_subscription').over(w))\
              .withColumn("month_user", F.sum('week_user').over(w))\
              .orderBy(F.col('month'), F.col('week'), F.col('city_id').cast(IntegerType()))
        
        self.df = self.df.withColumnRenamed('city_id', 'zone_id')
        
        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)    
                
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 

class Report4(object):

    df: DataFrame = None
    layer = 'dml'
    userCommonChannels: UserCommonChannels = None
    usersCorresponded: UsersCorresponded = None
    usersNear: UsersNear = None
    count: int = -1

    def __init__(self, session, userCommonChannels: UserCommonChannels, \
                            usersCorresponded: UsersCorresponded, \
                            usersNear: UsersNear):
        self.session = session
        self.path = self.__class__.__name__
        self.userCommonChannels = userCommonChannels
        self.usersCorresponded = usersCorresponded
        self.usersNear = usersNear

    def read(self):    
        if self.df is None:
            try: 
                self.df = ReaderCSV.read(self.session, self.layer, self.path)
            except Exception as E:
                print(f'{self.__class__.__name__} method "read" error') 
                raise E

            self.df = self.df.cache() ##!! cache
            self.df.rdd.setName(self.path)
            self.count = self.df.count()

    def calc(self, save: bool = True):
        
        if self.userCommonChannels is None:
            raise Exception(f'userCommonChannels in {self.__class__.__name__} is None') 
            
        if self.userCommonChannels.df is None:
            raise Exception(f'userCommonChannels.df in {self.__class__.__name__} is None') 

        if self.usersCorresponded is None:
            raise Exception(f'usersCorresponded in {self.__class__.__name__} is None') 
            
        if self.usersCorresponded.df is None:
            raise Exception(f'usersCorresponded.df in {self.__class__.__name__} is None') 

        if self.usersNear is None:
            raise Exception(f'usersNear in {self.__class__.__name__} is None') 
            
        if self.usersNear.df is None:
            raise Exception(f'usersNear.df in {self.__class__.__name__} is None') 

# UserCommonChannels
# root
#  |-- user_left: string (nullable = true)
#  |-- user_right: string (nullable = true)
#  |-- subscription_channel: long (nullable = true)

# UsersCorresponded
# root
#  |-- user_left: string (nullable = true)
#  |-- user_right: long (nullable = true)
            
# UsersNear
# root
#  |-- user_left: string (nullable = true)
#  |-- city_id_left: string (nullable = true)
#  |-- city_left: string (nullable = true)
#  |-- left_lat: double (nullable = true)
#  |-- left_lon: double (nullable = true)
#  |-- user_right: string (nullable = true)
#  |-- city_id_right: string (nullable = true)
#  |-- city_right: string (nullable = true)
#  |-- actual_lat: double (nullable = true)
#  |-- actual_lon: double (nullable = true)
#  |-- dist: double (nullable = true)

        self.userCommonChannels.df = self.userCommonChannels.df.withColumnRenamed('user_left', 'ucc_user_left')
        self.userCommonChannels.df = self.userCommonChannels.df.withColumnRenamed('user_right', 'ucc_user_right')

        # self.usersCorresponded.df = self.usersCorresponded.df.withColumnRenamed('user_left', 'uc_user_left')
        # self.usersCorresponded.df = self.usersCorresponded.df.withColumnRenamed('user_right', 'uc_user_right')

        self.df = self.usersNear.df.selectExpr('user_left', 'user_right', 'city_left', 'city_id_left as zone_id')

        # связь с userCommonChannels
        # inner
        self.df = self.df.join(self.userCommonChannels.df, \
                    [self.df.user_left == self.userCommonChannels.df.ucc_user_left, 
                     self.df.user_right == self.userCommonChannels.df.ucc_user_right], 'inner')

        # связь с usersCorresponded
        # leftanti
        self.df = self.df.join(self.usersCorresponded.df, \
                    [self.df.user_left == self.usersCorresponded.df.user_left, 
                     self.df.user_right == self.usersCorresponded.df.user_right], 'leftanti')

        self.df = self.df.withColumn('processed_dttm', F.current_timestamp())
        self.df = self.df.withColumn('local_time', \
                    F.when( F.col('city_left').isin('Sydney', 'Melbourne', 'Brisbane', 'Perth', \
                         'Adelaide', 'Canberra', 'Hobart', 'Darwin') \
                    , F.from_utc_timestamp(F.col('processed_dttm'), F.concat(F.lit('Australia/'), F.col('city_left'))))\
                     .otherwise(None))

        #self.df = self.df.where("uc_user_left is null")
        #self.df = self.df.drop('uc_user_left', 'uc_user_right', 'city_left')
        self.df = self.df.drop('city_left', 'subscription_channel', 'ucc_user_left', 'ucc_user_right')

        self.df = self.df.cache() ##!! cache
        self.df.rdd.setName(self.path)    
                
        if save:
            Saver.save(self.df, self.layer, self.path)
            
        self.count = self.df.count()
        
    def desc(self):
        print('Count:', self.count, 'Layer:', self.layer, 'Name:', self.path)
        if not self.df is None:
            print('RDD Name:', self.df.rdd.name()),
            print('Partitons cnt:', self.df.rdd.getNumPartitions())
            self.df.printSchema()
        else:
            print('self.df is None') 
