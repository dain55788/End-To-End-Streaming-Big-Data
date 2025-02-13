from spark_streaming_functions import *
from kafka_streaming.utils.kafka_utils import *
from cassandra_schema_creation import *

# Run the script using the following command to submit spark job (run in the spark_streaming directory)
# C:\spark\spark-3.4.4-bin-hadoop3\bin\spark-submit
# \ --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4 spark_streaming_events.py

# Kafka Topics
SALES_EVENTS_TOPIC = KAFKA_TOPICS["sales"]

# Default Kafka Address and Port
KAFKA_ADDRESS = "127.0.0.1"
KAFKA_PORT = "9092"
checkpoint_path = "/tmp/checkpoints"

# INITIALIZE SPARK CONNECTION
spark_con = create_spark_connection('SparkStreamingEvents')
spark_con.streams.resetTerminated()

# READING AND PROCESSING KAFKA STREAMING DATA
spark_df = connect_to_kafka(spark_con, SALES_EVENTS_TOPIC)
selection_df = create_selection_df_from_kafka(spark_df)

# CREATE CASSANDRA CONNECTION
session = create_cassandra_connection()

# DIMENSIONAL, FACT TABLES CREATION
create_keyspace(session)
create_all_dimension_tables(session)
create_sales_fact_table(session)

# WRITING DATA TO CASSANDRA DIMENSIONAL TABLES


# CREATE SPARK STREAMING TO CASSANDRA
logging.info("Streaming is being started...")
streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                   .option('checkpointLocation', '/tmp/checkpoint')
                   .option('keyspace', 'spark_streams')
                   .option('table', 'sales_fact'))

# START STREAMING
streaming_query.start()
streaming_query.awaitTermination()
