import os
from spark_streaming_functions import *
from schema import *
from kafka_streaming.events.kafka_utils import *
from src.integration import setup_warehouse, load_all_dimension_tables

# Run the script using the following command to submit spark job (run in the spark_streaming directory)
# %SPARK_HOME%\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4 spark_streaming_events.py

username = 'nguyendai'

# Kafka Topics
SALES_EVENTS_TOPIC = KAFKA_TOPICS["sales"]
PRODUCT_REVIEW_TOPIC = KAFKA_TOPICS["product_review"]
CUSTOMER_INTERACTION_TOPIC = KAFKA_TOPICS["customer_interaction"]

# Default Kafka Address and Port
KAFKA_ADDRESS = "127.0.0.1"
KAFKA_PORT = "9092"
checkpoint_path = "/tmp/checkpoints"


# Initialize Spark Session:
spark = create_spark_session('SparkStreamingEvents')
spark.streams.resetTerminated()

# READING AND PROCESSING KAFKA STREAM
# sales events stream
sales_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, SALES_EVENTS_TOPIC)
sales_events = process_stream(
    sales_events, *[fact_sales], SALES_EVENTS_TOPIC)

# product review events stream
product_review_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, PRODUCT_REVIEW_TOPIC)
product_review_events = process_stream(
    product_review_events, *[fact_product_review], PRODUCT_REVIEW_TOPIC)

# customer interaction events stream
customer_interation_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, CUSTOMER_INTERACTION_TOPIC)
customer_interation_events = process_stream(
    customer_interation_events, *[fact_customer_interaction], CUSTOMER_INTERACTION_TOPIC)

# EXECUTE INTEGRATION PROCESS
setup_warehouse()
load_all_dimension_tables()

# WRITING STREAM INTO HIVE
sales_events_writer = create_write_stream_to_hive(sales_events, checkpoint_path, fact_sales)
product_review_events_writer = create_write_stream_to_hive(product_review_events, checkpoint_path, fact_product_review)
customer_interation_events_writer = create_write_stream_to_hive(customer_interation_events, checkpoint_path,
                                                                fact_customer_interaction)

# START STREAMING (one time submit using spark-submit)
sales_events_writer.start()
product_review_events_writer.start()
customer_interation_events_writer.start()

spark.streams.awaitAnyTermination()

