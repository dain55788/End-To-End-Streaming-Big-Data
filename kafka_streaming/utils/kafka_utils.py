import os
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
import json
import logging

# Setting up Kafka Producer and Consumer
# File path configuration
current_script_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_path)
data_dir = os.path.join(os.path.dirname(project_root), 'data')

AMAZON_SALES_DATA = os.path.join(data_dir, "AmazonSaleReport.csv").replace("\\", "/")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Logging
logger = logging.getLogger(__name__)


def setup_logger(log_file="../log.txt"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logger


def create_kafka_producer():
    # Create and return a Kafka producer instance with default settings
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type="gzip",
            linger_ms=5,
            acks="all",
            api_version=(0, 10, 0)
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
