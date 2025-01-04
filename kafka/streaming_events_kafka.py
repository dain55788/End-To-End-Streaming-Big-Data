from kafka import KafkaProducer
import json
import pandas as pd
import numpy as np
from datetime import datetime
import time
import random
from typing import Dict, List
import os
import logging


# logging basic file config:-
logging.basicConfig(filename="log.txt", level=logging.DEBUG,
                    filemode='a',
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


# Adjust sales event, add product reviews and customer interactions events.
class SalesEventProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        current_script_path = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(os.path.dirname(current_script_path), 'data')

        customers_file_path = os.path.join(data_dir, "dim_customer.csv").replace("\\", "/")
        products_file_path = os.path.join(data_dir, "dim_product.csv").replace("\\", "/")
        locations_file_path = os.path.join(data_dir, "dim_location.csv").replace("\\", "/")
        payments_file_path = os.path.join(data_dir, "dim_payment.csv").replace("\\", "/")

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
            'date_id': int(datetime.now().strftime('%Y%m%d')),
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

    def generate_batch_sales(self, batch_size: int = 100) -> List[Dict]:
        """Generate a batch of sale events"""
        return [self.generate_single_sale() for _ in range(batch_size)]

    def send_sales_events(self, topic: str = 'sales_events', batch_size: int = 100):
        """Send batch of sales events to Kafka topic"""
        sales_batch = self.generate_batch_sales(batch_size)

        # Send each sale event to Kafka
        for sale in sales_batch:
            try:
                self.producer.send(topic, value=sale)
            except Exception as e:
                print(f"Error sending sale {sale['sale_id']}: {str(e)}")
                logging.error(e)
        self.producer.flush()

        print(f"Sent {batch_size} sales events at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def start_streaming(self, interval: int = 2, batch_size: int = 100):
        """Start streaming sales events at specified interval"""
        try:
            print(f"Starting sales event streaming to Kafka...")
            logging.info(f"Starting sales event streaming to Kafka...")
            print(f"Batch size: {batch_size} events every {interval} seconds")
            logging.info(f"Batch size: {batch_size} events every {interval} seconds")
            while True:
                self.send_sales_events(batch_size=batch_size)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nStopping sales event streaming...")
            self.producer.close()
            print("Producer closed successfully")


if __name__ == "__main__":
    # Configure Kafka producer settings
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_TOPIC = 'sales_events'
    BATCH_SIZE = 100
    INTERVAL_SECONDS = 2

    # Create and start producer
    try:
        producer = SalesEventProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        producer.start_streaming(
            interval=INTERVAL_SECONDS,
            batch_size=BATCH_SIZE
        )
    except Exception as e:
        print(f"Error: {str(e)}")

