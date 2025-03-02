# End-To-End-Streaming-Big-Data
## End-To-End Streaming Big Data Project makes big data processing easy with Airflow, Kafka, Spark, MinIO and much more!!

## Top Contents:
+ Streaming Big Amount of Data using Kafka and SparkStreaming.
+ Managing Apache Kafka with Confluent Control Center, Apache Zookeeper and Schema Registry.
+ Processing Data Lake using DeltaLake, Object Storage with MinIO.
+ ELT Pipeline:
  + Automated Medallion Architecture Implementation on the dataset using Dbt and Airflow.
  + Data Modeling with PostgreSQL and Dbt
+ Distributed query engine Trino with DBeaver for high query performance.
+ Data Visualization Tools with Superset.
+ Project Report.

## Dataset:
This project uses Amazon Sales Report data, you can find the data here: https://github.com/AshaoluV/Amazon-Sales-Project/blob/main/Amazon%20Sales.csv

## Star Schema Model

## Tools & Technologies
+ Streaming Data Process: Apache Kafka, Apache Spark.
+ IDE: Pycharm
+ Programming Languages: Python.
+ Data Orchestration Tool: Apache Airflow.
+ Data Lake/ Data Lakehouse: DeltaLake, MinIO.
+ Data Visualization Tool: Superset.
+ Containerization: Docker, Docker Compose.
+ Query Engine: DBeaver, Trino.
+ Data Transformation and Data Warehousing: dbt, PostgreSQL

## Architecture
![SystemArchitecture](https://github.com/user-attachments/assets/09d4f66f-62ec-47fc-a7a5-e872c35eca49)


## Setup
### Pre-requisites: 
+ First, you'll have your Pycharm IDE, Docker, Apache Kafka, Apache Spark and Apache Airflow setup in your project.
+ In your terminal, create a python virtual environment to work with, run (if you are using Windows):
1. ```python -m venv venv```
2. ```venv\Scripts\activate```
3. ```python -m pip install -r requirements.txt``` (download all required libraries for the project)
+ Launch Docker: ```docker compose up -d```
+ Run event_streaming python file in Kafka events.
4. Run the command: python spark_streaming/sales_delta_spark_to_minio.py (submiting spark job and stream the data to MinIO)
5. Access the service:
  + Confluent Control Center for Kafka is accessible at `http://localhost:9021`.

![ConfluentControlCenter](https://github.com/user-attachments/assets/afb99b8d-e892-4055-93de-16863da25c61)


  + MinIO is accessible at `http://localhost:9001`.

![MinIO-UI](https://github.com/user-attachments/assets/be80d18f-8a63-4bf1-9bfc-1e1bdc6c9bdf)

  + Trino is accessible at `http://localhost:8084`.

![Trino-UI](https://github.com/user-attachments/assets/704a5de4-2c07-46a4-ac23-ecdd509ebc2d)


### How can I make this better?!
A lot can still be done :)
+ Choose managed Infra
  + Cloud Composer for Airflow, Kafka and Spark using AWS.
+ Kafka Streaming process monitering with Prometheus and Grafana.
+ Include CI/CD Operations.
+ Write data quality tests.
+ Storage Layer Deployment with AWS S3 and Terraform.

---
<p>&copy; 2025 Nguyen  Dai</p>
