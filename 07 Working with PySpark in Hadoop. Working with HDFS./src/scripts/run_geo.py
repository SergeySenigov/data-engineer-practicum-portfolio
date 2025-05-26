import os, sys
 
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType
from pyspark.storagelevel import StorageLevel
import pr7_classes as pr7


def main():

    task = sys.argv[1]

    print("--------===== Start  =====--------")
    
    try:
        spark = pr7.get_session()
        
        print("--------===== Started Spark  =====--------")

        pr7.print_conf(spark)

        print ("Passed task name:", task)

        if task == 'Cities':
            print ("Working with ", task)

            citiesRaw = pr7.CitiesRaw(spark)
            citiesRaw.calc(True)
            citiesRaw.desc()
             
            cities = pr7.Cities(spark, citiesRaw)
            cities.calc(True)
            cities.desc()
            return 


        if task == 'EventsWithUserAndCoords':
            print ("Working with ", task)

            eventsSource = pr7.EventsSource(spark)
            eventsSource.read('2022-04-01')
            eventsSource.desc()

            eventsRaw = pr7.EventsRaw(spark, eventsSource)
            eventsRaw.calc(True)
            eventsRaw.desc()

            eventsWithUserAndCoords = pr7.EventsWithUserAndCoords(spark, eventsRaw) 
            eventsWithUserAndCoords.calc(True)
            eventsWithUserAndCoords.desc()
            return

        if task == 'EventsWithCitiesAll':
            print ("Working with ", task)

            eventsWithUserAndCoords = pr7.EventsWithUserAndCoords(spark, None) 
            eventsWithUserAndCoords.read()
            eventsWithUserAndCoords.desc()

            cities = pr7.Cities(spark, None)
            cities.read()
            cities.desc()

            eventsWithCitiesPartial = pr7.EventsWithCitiesPartial(spark, eventsWithUserAndCoords, cities) 
            eventsWithCitiesPartial.calc(True)
            eventsWithCitiesPartial.desc()

            eventsWithCitiesAll = pr7.EventsWithCitiesAll(spark, eventsWithCitiesPartial) 
            eventsWithCitiesAll.calc(True)
            eventsWithCitiesAll.desc()
            return 


        if task == 'EventsWithRegsWithCities':
            print ("Working with ", task)

            eventsWithCitiesAll = pr7.EventsWithCitiesAll(spark, None) 
            eventsWithCitiesAll.read()
            eventsWithCitiesAll.desc()

            registrationsWithCities = pr7.RegistrationsWithCities(spark, eventsWithCitiesAll)
            registrationsWithCities.calc(True)
            registrationsWithCities.desc()

            eventsWithRegsWithCities = pr7.EventsWithRegsWithCities(spark, eventsWithCitiesAll, registrationsWithCities)
            eventsWithRegsWithCities.calc(True)
            eventsWithRegsWithCities.desc()
            return 
            

        if task == 'Users':
            print ("Working with ", task)
            
            eventsWithCitiesAll = pr7.EventsWithCitiesAll(spark, None) 
            eventsWithCitiesAll.read()
            eventsWithCitiesAll.desc()
            
            users = pr7.Users(spark, eventsWithCitiesAll)
            users.calc(True)
            users.desc()
            return 


        if task == 'UserTravels':
            print ("Working with ", task)

            eventsWithCitiesAll = pr7.EventsWithCitiesAll(spark, None) 
            eventsWithCitiesAll.read()
            eventsWithCitiesAll.desc()

            userTravelCities = pr7.UserTravelCities(spark, eventsWithCitiesAll)
            userTravelCities.calc(True)
            userTravelCities.desc()

            userTravels = pr7.UserTravels(spark, userTravelCities)
            userTravels.calc(True)
            userTravels.desc()
            return 

        if task == 'Report2':
            print ("Working with ", task)
            
            users = pr7.Users(spark, None)
            users.read()
            users.desc()            
            
            userTravels = pr7.UserTravels(spark, None)
            userTravels.read()
            userTravels.desc()
            
            report2 = pr7.Report2(spark, users, userTravels)
            report2.calc(True)
            report2.desc()
            return 
        
        if task == 'Report3':
            print ("Working with ", task)

            eventsWithRegsWithCities = pr7.EventsWithRegsWithCities(spark, None, None)
            eventsWithRegsWithCities.read()
            eventsWithRegsWithCities.desc()

            report3 = pr7.Report3(spark, eventsWithRegsWithCities)
            report3.calc(True)
            report3.desc()
            return 
        
        if task == 'Report4':
            print ("Working with ", task)
            
            eventsWithUserAndCoords = pr7.EventsWithUserAndCoords(spark, None)
            eventsWithUserAndCoords.read()
            eventsWithUserAndCoords.desc()

            userChannelSubscriptions = pr7.UserChannelSubscriptions(spark, eventsWithUserAndCoords)
            userChannelSubscriptions.calc(True, 0.001)
            userChannelSubscriptions.desc()

            userCommonChannels = pr7.UserCommonChannels(spark, userChannelSubscriptions, userChannelSubscriptions)
            userCommonChannels.calc(True)
            userCommonChannels.desc()

            
            usersCorresponded = pr7.UsersCorresponded(spark, eventsWithUserAndCoords)
            usersCorresponded.calc(True)
            usersCorresponded.desc()


            usersLeft = pr7.Users(spark, None)
            usersLeft.read()
            usersLeft.desc()

            usersRight = pr7.Users(spark, None)
            usersRight.read()
            usersRight.desc()

            usersNear = pr7.UsersNear(spark, usersLeft, usersRight)
            usersNear.calc(True)
            usersNear.desc()

            report4 = pr7.Report4(spark, userCommonChannels, usersCorresponded, usersNear)
            report4.calc(True)
            report4.desc()

            return 
        
        raise Exception(f"Unknown task: {task}")
        
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise
    
    finally:
        print("--------===  End   =====--------")


if __name__ == "__main__":
    main()