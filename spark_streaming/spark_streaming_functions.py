import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys
import warnings
import traceback
from delta import *


# Run python spark_streaming/orders_delta_spark_to_minio.py to push data to MinIO (in Delta format)
# SPARK CONNECTION
def create_spark_session():
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
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark_con


def load_minio_config(spark_context: SparkContext):
    """
    Established the necessary configurations to access to MinIO
    """
    try:
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_access_key")
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_secret_key")
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_context.jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.warning(f"MinIO config could not be created successfully due to exception: {e}")


# READING STREAM FROM KAFKA
def connect_to_kafka(spark_session, topic):
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
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created due to: {e}")

    return spark_df


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


def start_streaming(df):
    """
    :param
    Input:
    Converts data to delta lake format and store into MinIO
    """
    minio_bucket = "lakehouse"

    logging.info("Streaming is being started...")
    stream_query = df.writeStream \
                        .format("delta") \
                        .outputMode("append") \
                        .option("checkpointLocation", f"minio_streaming/{minio_bucket}/sales/checkpoints") \
                        .option("path", f"s3a://{minio_bucket}/sales") \
                        .partitionBy("store") \
                        .start()

    return stream_query.awaitTermination()


# Main execution
if __name__ == '__main__':
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
