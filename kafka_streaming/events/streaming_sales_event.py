from kafka import KafkaProducer
import json
import pandas as pd
import numpy as np
from datetime import datetime
import time
import random
from typing import Dict, List
import os
from kafka_utils import *

# logger basic file core:
logger = setup_logger()


class SalesEventProducer:
    def __init__(self):
        # Initialize Kafka producer (from Kafka utils)
        self.producer = create_kafka_producer()

        # Load dimension data
        self.customers = pd.read_csv(customers_file_path)
        self.products = pd.read_csv(products_file_path)
        self.locations = pd.read_csv(locations_file_path)
        self.payments = pd.read_csv(payments_file_path)

        # Initialize sale_id counter
        self.sale_id = 1

    def generate_single_sale(self) -> Dict:
        """Generate a single sale event"""
        # Select random product
        product = self.products.iloc[np.random.randint(len(self.products))]

        # Calculate pricing
        quantity = np.random.randint(1, 5)
        unit_price = float(product['base_price']) * np.random.uniform(0.9, 1.2)
        subtotal = unit_price * quantity
        discount = np.random.choice([0, 5, 10, 15, 20], p=[0.6, 0.2, 0.1, 0.05, 0.05])
        discount_amount = subtotal * (discount / 100)
        tax = (subtotal - discount_amount) * 0.1
        total_amount = subtotal - discount_amount + tax

        # Create sale event
        sale = {
            'sale_id': self.sale_id,
            'order_id': self.sale_id,  # 1 order = 1 sale
            'product_id': int(product['product_id']),
            'customer_id': int(self.customers.iloc[np.random.randint(len(self.customers))]['customer_id']),
            'date_id': datetime.now().strftime('%Y%m%d'),
            'location_id': int(self.locations.iloc[np.random.randint(len(self.locations))]['location_id']),
            'payment_id': int(self.payments.iloc[np.random.randint(len(self.payments))]['payment_id']),
            'amount': round(float(subtotal), 2),
            'quantity': int(quantity),
            'unit_price': round(float(unit_price), 2),
            'discount': float(discount),
            'tax': round(float(tax), 2),
            'total_amount': round(float(total_amount), 2),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        self.sale_id += 1
        return sale

    def generate_batch_sales(self, batch_size) -> List[Dict]:
        # Generate a batch of sale events
        return [self.generate_single_sale() for _ in range(batch_size)]

    def send_sales_events(self, topic, batch_size):
        # Send batch of sales events to Kafka topic
        sales_batch = self.generate_batch_sales(batch_size)

        # Send each sale event to Kafka
        try:
            success = send_to_kafka(
                self.producer,
                KAFKA_TOPICS[topic],
                sales_batch,
                batch=True
            )
            if success:
                print(f"Sent {batch_size} sales events at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                logger.error("Failed to send sales batch")
        except Exception as e:
            logger.error(f"Error in send_sales_events: {str(e)}")

    def start_streaming(self, interval, batch_size):
        # Start streaming sales events at specified interval
        try:
            print(f"Starting sales event streaming to Kafka...")
            print(f"Batch size: {batch_size} events every {interval} seconds")
            while True:
                self.send_sales_events('sales', batch_size)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nStopping sales event streaming...")
            close_producer(self.producer)
            print("Producer closed successfully")
        except Exception as e:
            print(f"Failed closing Kafka producer because of error {str(e)}")
            logger.error(e)


if __name__ == "__main__":
    # Configure Kafka producer settings
    BATCH_SIZE = 100  # Modify back to 10000
    INTERVAL_SECONDS = 4

    # Create and start producer
    try:
        producer = SalesEventProducer()
        producer.start_streaming(
            interval=INTERVAL_SECONDS,
            batch_size=BATCH_SIZE
        )
    except Exception as e:
        print(f"Main Execution Error: {str(e)}")

