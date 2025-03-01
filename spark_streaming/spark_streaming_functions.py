import logging
from pyspark import SparkConf, SparkContext
import sys
import warnings
import traceback
from delta import *

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')


# Run python spark_streaming/orders_delta_spark_to_minio.py to push data to MinIO (in Delta format)
# Create the MinIO bucket before running this
# SPARK CONNECTION
def create_spark_session():
    from pyspark.sql import SparkSession
    """
    :param
    Target: Creates the Spark Session with suitable configs
    Input: Application name
    Output: Spark session
    """
    spark_con = None
    try:
        builder = SparkSession.builder \
            .appName("Streaming Kafka") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark_con = configure_spark_with_delta_pip(builder, extra_packages=[
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4,org.apache.hadoop:hadoop-aws:2.8.2"]).getOrCreate()
        # First jar: enable connection between Spark and Kafka, the latter enable Spark to write data to AWS Services
        spark_con.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark_con


# MINIO CONFIGURATION NEEDED FOR CONNECTION TO SPARK
def load_minio_config(spark_context: SparkContext):
    """
    Established the necessary configurations to access to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_access_key")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_secret_key")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.warning(f"MinIO config could not be created successfully due to exception: {e}")


# CREATING INITIAL DATAFRAME STREAM FROM KAFKA TO SPARK
def create_initial_dataframe(spark_session, topic):
    """
    :param spark_session: spark connection (spark session)
    :param topic: Kafka topics
    :return: spark_df: spark connection to kafka
    """
    spark_df = None
    try:
        spark_df = spark_session.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', topic) \
            .option("failOnDataLoss", "false") \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Initial Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created due to: {e}")

    return spark_df


# CREATING FINAL DATAFRAME STREAM FROM KAFKA TO SPARK
def creare_final_dataframe(df):
    """
    :param df:
    :return: final dataframe that spark uses to load to minio
    target: modify the initial dataframe to create final dataframe
    """
    from pyspark.sql.functions import from_json, col, current_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    schema = StructType([
        StructField("index", IntegerType(), False),
        StructField("Order ID", StringType(), False),
        StructField("Date", StringType(), False),
        StructField("Status", StringType(), False),
        StructField("Fulfilment", StringType(), False),
        StructField("Sales Channel", StringType(), False),
        StructField("ship-service-level", StringType(), True),
        StructField("Style", StringType(), False),
        StructField("SKU", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("Size", StringType(), False),
        StructField("ASIN", StringType(), False),
        StructField("Courier Status", StringType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("currency", StringType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("ship-city", StringType(), True),
        StructField("ship-state", StringType(), True),
        StructField("ship-postal-code", StringType(), True),
        StructField("ship-country", StringType(), True),
        StructField("promotion-ids", StringType(), True),
        StructField("B2B", StringType(), True),
        StructField("fulfilled-by", StringType(), True)
    ])

    # Parse JSON from Kafka and apply schema
    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingestion_timestamp", current_timestamp())

    return parsed_df


def start_streaming(df):
    """
    :param
    Input:
    Converts data to delta lake format and store into MinIO
    """
    minio_bucket = "lakehouse"
    bronze_layer_path = f"s3a://{minio_bucket}/sales_bronze"

    logging.info("Streaming is being started...")
    stream_query = df.writeStream \
                        .format("delta") \
                        .outputMode("append") \
                        .option("checkpointLocation", f"s3a://{minio_bucket}/checkpoints/sales_bronze") \
                        .option("path", bronze_layer_path) \
                        .partitionBy("Date") \
                        .trigger(processingTime="1 minute") \
                        .start()

    return stream_query.awaitTermination()
