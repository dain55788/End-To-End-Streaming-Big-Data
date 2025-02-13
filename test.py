from kafka_streaming.utils.kafka_utils import *

# Run the script using the following command to submit spark job (run in the spark_streaming directory)
# C:\spark\spark-3.4.4-bin-hadoop3\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4 spark_streaming_events.py

username = 'nguyendai'

# Kafka Topics
SALES_EVENTS_TOPIC = KAFKA_TOPICS["sales"]
print(SALES_EVENTS_TOPIC)