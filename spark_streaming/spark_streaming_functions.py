# Create write stream and directly write stream to Hive every 1 minute
#
# def create_write_stream_to_hive(processed_stream, checkpoint_path, table_name, trigger_interval="1 minutes"):
#     """
#     Create write stream to Hive table
#     Parameters:
#         processed_stream: DataFrame
#         checkpoint_path: str
#         table_name: str
#         trigger_interval: str
#     Returns:
#         streaming_query: StreamingQuery
#     """
#     try:
#         def write_to_hive(batch_df, batch_id):
#             if not batch_df.isEmpty():
#                 batch_df.write.mode("append").saveAsTable(table_name)
#
#         streaming_query = (processed_stream
#                            .writeStream
#                            .format("hive")
#                            .foreachBatch(write_to_hive)
#                            .option("checkpointLocation", f"{checkpoint_path}/{table_name}")
#                            .trigger(processingTime=trigger_interval)
#                            .toTable(table_name)
#                            .start())
#
#         logging.info(f"Successfully created write stream to Hive table: {table_name}")
#         return streaming_query
#     except Exception as e:
#         logging.error(f"Failed to create write stream to Hive because of Error: {e}")
#         raise

import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# CREATE CASSANDRA KEYSPACE
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '5'};
    """)

    print("Keyspace created successfully!")


# # INSERT DATA FROM SPARK STREAMING TO CASSANDRA
# def insert_data(session, **kwargs):
#     print("inserting data...")
#
#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')
#
#     try:
#         session.execute("""
#             INSERT INTO spark_streams.sales_fact(id, first_name, last_name, gender, address,
#                 post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (user_id, first_name, last_name, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {first_name} {last_name}")
#
#     except Exception as e:
#         logging.error(f'Could not insert data due to {e}')


# SPARK CONNECTION
def create_spark_connection(app_name):
    spark_con = None

    try:
        spark_con = SparkSession.builder \
            .appName(app_name) \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.4,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark_con.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_con


# KAFKA CONNECTION TO SPARK
def connect_to_kafka(spark_con, topic):
    spark_df = None
    try:
        spark_df = spark_con.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', topic) \
            .option("failOnDataLoss", False) \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


# SPARK CONNECTION TO CASSANDRA
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


# CREATING DATAFRAME FROM KAFKA TO SPARK DATA FRAME
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("product_key", IntegerType(), False),
        StructField("time_key", IntegerType(), False),
        StructField("date_key", IntegerType(), False),
        StructField("warehouse_key", IntegerType(), False),
        StructField("promotion_key", IntegerType(), False),
        StructField("customer_key", IntegerType(), False),
        StructField("location_key", IntegerType(), False),
        StructField("website_key", IntegerType(), False),
        StructField("navigation_key", IntegerType(), False),
        StructField("ship_mode_key", IntegerType(), False),
        StructField("total_order_quantity", IntegerType(), False),
        StructField("line_item_discount_amount", DoubleType(), False),
        StructField("line_item_sale_amount", DoubleType(), False),
        StructField("line_item_list_price", DoubleType(), False),
        StructField("average_line_item_sale", DoubleType(), False),
        StructField("average_line_item_list_price", DoubleType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return sel


# INSERTING DATA TO DIMENSIONAL TABLES

