# End-To-End-Streaming-Big-Data
## End-To-End Streaming Big Data Project makes processing big data easy with Airflow, Kafka, Spark, Apache Hive and much more!!

## Top Contents:
+ Streaming Big Amount of Data using Kafka and SparkStreaming.
+ Managing Apache Kafka with Confluent Control Center, Apache Zookeeper and Schema Registry.
+ Automated Medallion Architecture using Data Orchestration Tools (Apache Airflow)
+ Processing Data Lake using DeltaLake, Object Storage with MinIO.
+ Distributed query engine Trino with DBeaver for high query performance.
+ Data Visualization Tools with Superset.
+ Project Report.

## Dataset:
This project uses fake created fact data related to e-commerce platform while streaming data with Kafka.

## Star Schema Model
![schema_model](https://github.com/user-attachments/assets/4727ee2f-8403-4c20-b473-b9a28553ca9b)

## Tools & Technologies
+ Streaming Data Process: Apache Kafka, Apache Spark.
+ IDE: Pycharm
+ Programming Languages: Python.
+ Data Orchestration Tool: Apache Airflow
+ Data Lake/ Data Lakehouse: DeltaLake, MinIO
+ Data Visualization Tool: Superset
+ Containerization: Docker, Docker Compose.
+ Query Engine: DBeaver, Trino

## Architecture
![SystemArchitecture](https://github.com/user-attachments/assets/3214fefd-cd58-433e-a29b-8b2c3d6c5bff)

## Setup
### Pre-requisites: 
+ First, you'll have your Pycharm IDE, Docker, Apache Kafka, Apache Spark and Apache Airflow setup in your project.
+ In your terminal, create a python virtual environment to work with, run (if you are using Windows):
1. python -m venv venv
2. venv\Scripts\activate
+ Launch Docker, run event_streaming python file in Kafka events.

### How can I make this better?!
A lot can still be done :)
+ Choose managed Infra
  + Cloud Composer for Airflow, Kafka and Spark using AWS.
+ Kafka Streaming process monitering with Prometheus and Grafana.
+ Include CI/CD Operations.
+ OLAP Operations for higher query performance with data warehouse (Snowflake, Clickhouse).
+ Write data quality tests.
+ Storage Layer Deployment with AWS S3 and Terraform.
