import os
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
import json
import logging

# File path configuration
current_script_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_path)
data_dir = os.path.join(os.path.dirname(project_root), 'data')

customers_file_path = os.path.join(data_dir, "customer_dimension.csv").replace("\\", "/")
products_file_path = os.path.join(data_dir, "product_dimension.csv").replace("\\", "/")
locations_file_path = os.path.join(data_dir, "location_dimension.csv").replace("\\", "/")
date_file_path = os.path.join(data_dir, "date_dimension.csv").replace("\\", "/")
time_file_path = os.path.join(data_dir, "time_dimension.csv").replace("\\", "/")
promotion_file_path = os.path.join(data_dir, "promotion_dimension.csv").replace("\\", "/")
website_file_path = os.path.join(data_dir, "website_dimension.csv").replace("\\", "/")
navigation_file_path = os.path.join(data_dir, "navigation_dimension.csv").replace("\\", "/")
warehouse_file_path = os.path.join(data_dir, "warehouse_dimension.csv").replace("\\", "/")
ship_mode_file_path = os.path.join(data_dir, "ship_mode_dimension.csv").replace("\\", "/")


# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPICS = {
    'sales': 'sales_events',
}


def setup_logger(log_file="../log.txt", max_size_bytes=1024*1024*10, backup_count=3):
    # Configure and return a logger instance
    # Create a logger
    logger = logging.getLogger('KafkaStreamLogger')
    logger.setLevel(logging.INFO)

    # Clear any existing handlers to prevent duplicate logging
    logger.handlers.clear()

    # Create a rotating file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_size_bytes,  # 10 MB per file
        backupCount=backup_count  # Keep 3 backup files
    )

    # Create a formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

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
