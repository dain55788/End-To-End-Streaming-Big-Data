from spark_streaming_functions import *
from kafka_streaming.utils.kafka_utils import *

# Kafka Topics
AMAZON_SALES = AMAZON_SALES_DATA

# Default Kafka Address and Port
KAFKA_ADDRESS = "127.0.0.1"
KAFKA_PORT = "9092"
checkpoint_path = "/tmp/checkpoints"

# INITIALIZE SPARK CONNECTION
spark_con = create_spark_session()

# LOAD MINIO CONFIG
load_minio_config(spark_con.sparkContext)

# READING AND PROCESSING KAFKA STREAMING DATA
initial_df = create_initial_dataframe(spark_con, AMAZON_SALES)
final_df = creare_final_dataframe(initial_df, AMAZON_SALES)

# START STREAMING
start_streaming(final_df)
