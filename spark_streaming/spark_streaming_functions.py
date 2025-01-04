import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


# logging basic file config:-
logging.basicConfig(filename="log.txt",level=logging.DEBUG,
                    filemode='a',
                    format= '%(asctime)s - %(message)s',
                    datefmt= '%d-%b-%y %H:%M:%S')


# Create Spark Session function
def create_spark_connection():
    spark_con = None
    try:
        spark_con = SparkSession.builder \
                .appName('SparkSalesEventsStreaming') \
                .config('spark.jars.packages', "org.apache.spark:sparl-sql-kafka-0-10_2.13:3.4.4") \
                .enableHiveSupport() \
                .getOrCreate()

        spark_con.sparkContext.setLogLevel("ERROR")
        logging.info(f"Successfully create Spark Session!!")
    except Exception as e:
        logging.error(f"Couldn't create spark session due to exception {e}!!")

    return spark_con


# Create Spark Connection to Hive
# def create_hive_connection():

