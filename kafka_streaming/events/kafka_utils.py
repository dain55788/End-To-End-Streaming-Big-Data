import os
from kafka import KafkaProducer
import json
import logging

# File path
current_script_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_path)
data_dir = os.path.join(os.path.dirname(project_root), 'data')

customers_file_path = os.path.join(data_dir, "dim_customer.csv").replace("\\", "/")
products_file_path = os.path.join(data_dir, "dim_product.csv").replace("\\", "/")
locations_file_path = os.path.join(data_dir, "dim_location.csv").replace("\\", "/")
payments_file_path = os.path.join(data_dir, "dim_payment.csv").replace("\\", "/")
date_file_path = os.path.join(data_dir, "dim_date.csv").replace("\\", "/")
channel_file_path = os.path.join(data_dir, "dim_channel.csv").replace("\\", "/")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPICS = {
    'sales': 'sales_events',
    'customer_interaction': 'customer_interactions',
    'product_review': 'product_reviews'
}


def setup_logger(log_file="../log.txt"):
    # Configure and return a logger instance
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        filemode='a',
        format='%(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S'
    )
    return logging.getLogger(__name__)


def create_kafka_producer():
    # Create and return a Kafka producer instance with default settings
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(0, 10, 1)
        )
    except Exception as e:
        logging.error(f"Error creating Kafka producer: {str(e)}")
        raise


def send_to_kafka(producer, topic, data, batch=False):
    """
    Send data to Kafka topic

    Args:
        producer: KafkaProducer instance
        topic: Target Kafka topic
        data: Single message or list of messages
        batch: Boolean indicating if data is a batch of messages
    """
    try:
        if batch:
            for message in data:
                producer.send(topic, value=message)
        else:
            producer.send(topic, value=data)
        producer.flush()
        return True
    except Exception as e:
        logging.error(f"Error sending to Kafka: {str(e)}")
        return False


def close_producer(producer):
    # Safely close Kafka producer
    try:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed successfully")
    except Exception as e:
        logging.error(f"Error closing Kafka producer: {str(e)}")
