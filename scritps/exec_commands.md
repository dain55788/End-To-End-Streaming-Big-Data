#Submit Spark job

#Stream all events into Spark, and Spark to Hive
nohup spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4 \
spark_streaming_events.py \

> nohup.out 2>&1 &
