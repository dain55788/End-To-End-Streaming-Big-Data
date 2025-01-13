import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyhive import hive
from pyspark.sql.functions import from_json, col
import os
from os.path import abspath

# JAVA_HOME Congif
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jdk-17"

# logging basic file core:-
logging.basicConfig(filename="log.txt", level=logging.DEBUG,
                    filemode='a',
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


# Create Spark Session function
def create_spark_session(app_name, master="spark://spark-master", enable_hive=True, hive_metastore_uri=None):
    # Create or get spark session with yarn master
    """
        Create Spark Session using builder for configuration
        Parameters:
            app_name: str
            master: yarn
            enable_hive: boolean
            hive_metastore_uri: str
        Returns:
            spark_con: Spark Session
        Connection: SparkSQL-Kafka, Spark-Hive, Spark-HiveMetastore
    """

    # NOTE: FIX THE WAREHOUSE_LOCATION VARIABLE AND CONTINUE TO CONNECT SPARK TO HIVE METASTORE
    spark_con = None
    warehouse_location = abspath('spark_warehouse')
    try:
        # Configuration for Spark
        spark_builder = SparkSession.builder \
            .appName(app_name) \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka_streaming-0-10_2.13:3.4.4,"
                                           "org.apache.hive:hive-metastore:4.0.1") \
            .config("spark.driver.extraJavaOptions", "-XX:+ShowCodeDetailsInExceptionMessages") \
            .enableHiveSupport() \
            .master(master)

        if hive_metastore_uri:
            spark_builder = spark_builder.config('hive.metastore.uris', hive_metastore_uri)

        # Configuration for Hive Metastore
        spark_builder = spark_builder.config("spark.sql.warehouse.dir", warehouse_location)\
            .config("spark.sql.catalogImplementation", "hive")

        spark_con = spark_builder.getOrCreate()
        spark_con.sparkContext.setLogLevel("ERROR")
        logging.info(f"Successfully create Spark Session!!")
        return spark_con
    except Exception as e:
        logging.error(f"Couldn't create spark session due to exception {e}!!")
        raise


# Create Spark Connection to Hive
def create_hive_connection(username):
    connect = hive.Connection(host="127.0.0.1", port="10000", username=username, database='default')
    return connect


# Create read stream from Kafka
def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
    """
        Creates a kafka_streaming read stream
        Parameters:
            spark : SparkSession
            kafka_address: str (Host address of the kafka_streaming bootstrap server)
            kafka_port: Kafka Connection Port
            topic : str (Name of the kafka_streaming topic)
            starting_offset: str (Starting offset configuration, "earliest" by default)
        Returns:
            read_stream: DataStreamReader
        """
    try:
        read_stream = (spark
                        .readStream
                        .format("kafka")
                        .option("kafka_streaming.bootstrap.servers", f"{kafka_address}:{kafka_port}")
                        .option("failOnDataLoss", False)
                        .option("startingOffsets", starting_offset)
                        .option("subscribe", topic)
                        .option("maxOffsetsPerTrigger", 100000)
                        .load())
        logging.info(f"Successfully create Read stream from Kafka!!")
        return read_stream
    except Exception as e:
        logging.error(f"Fail to create read stream from kafka_streaming with error {e}")
        raise


# Process read stream from Kafka
def process_stream(stream, stream_schema, topic):
    """
        Process stream to fetch on value from the kafka_streaming message.
        Parameters:
            stream : DataStreamReader
                The data stream reader for your stream
            stream_schema: StructType
                Schema definition for the Kafka message value
            topic: str
                Kafka topic name for logging purposes
        Returns:
            stream: DataStreamReader
    """
    try:
        # extract the 'value' field and cast to string
        stream = stream.selectExpr("CAST(value AS STRING)")

        # parse JSON messages using the provided schema
        processed_stream = stream.select(from_json(col("value"), stream_schema)
                                         .alias("data")).select("data.*")

        logging.info(f"Successfully processed stream from topic: {topic}")
        return processed_stream

    except Exception as e:
        logging.error(f"Failed to process stream from topic {topic} with error: {e}")
        return None


# Create write stream and directly write stream to Hive every 1 minute

def create_write_stream_to_hive(processed_stream, checkpoint_path, table_name, trigger_interval="1 minutes"):
    """
    Create write stream to Hive table
    Parameters:
        processed_stream: DataFrame
        checkpoint_path: str
        table_name: str
        trigger_interval: str
    Returns:
        streaming_query: StreamingQuery
    """
    try:
        def write_to_hive(batch_df, batch_id):
            if not batch_df.isEmpty():
                batch_df.write.mode("append").saveAsTable(table_name)

        streaming_query = (processed_stream
                           .writeStream
                           .format("hive")
                           .foreachBatch(write_to_hive)
                           .option("checkpointLocation", f"{checkpoint_path}/{table_name}")
                           .trigger(processingTime=trigger_interval)
                           .toTable(table_name)
                           .start())

        logging.info(f"Successfully created write stream to Hive table: {table_name}")
        return streaming_query
    except Exception as e:
        logging.error(f"Failed to create write stream to Hive because of Error: {e}")
        raise
