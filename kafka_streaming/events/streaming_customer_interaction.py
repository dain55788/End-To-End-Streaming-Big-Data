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


class ProductReviewProducer:
    def __init__(self):
        # Initialize Kafka producer (from Kafka utils)
        self.producer = create_kafka_producer()

        # Load dimension data
        self.customers = pd.read_csv(customers_file_path)
        self.products = pd.read_csv(products_file_path)
        self.date = pd.read_csv(date_file_path)
        self.channels = pd.read_csv(channel_file_path)

        # Initialize sale_id counter
        self.interaction_id = 1

    def generate_single_interaction(self) -> Dict:
        """Generate a single customer interaction event"""
        # Select random customer and channel
        customer = self.customers.iloc[np.random.randint(len(self.customers))]
        channel = self.channels.iloc[np.random.randint(len(self.channels))]

        # Define interaction types and details based on channel
        channel_interactions = {
            'Website': {
                'types': ['Browse', 'Purchase', 'Account Update', 'Technical Issue', 'Return Request'],
                'details': {
                    'Browse': ['Product search', 'Category browsing', 'Price comparison', 'Wishlist management'],
                    'Purchase': ['Checkout process', 'Payment issue', 'Order confirmation', 'Shipping details'],
                    'Account Update': ['Password reset', 'Address update', 'Profile modification', 'Preferences update'],
                    'Technical Issue': ['Page error', 'Loading problem', 'Cart issues', 'Payment gateway error'],
                    'Return Request': ['Return initiation', 'Return label request', 'Refund status check']
                }
            },
            'Mobile App': {
                'types': ['App Usage', 'Purchase', 'Technical Issue', 'Account Management', 'Notification'],
                'details': {
                    'App Usage': ['App navigation', 'Feature exploration', 'Product search', 'App crash report'],
                    'Purchase': ['Mobile checkout', 'Payment processing', 'Order tracking', 'In-app purchase'],
                    'Technical Issue': ['App crash', 'Login problem', 'Feature malfunction', 'Update issue'],
                    'Account Management': ['Profile update', 'Settings change', 'Preferences modification'],
                    'Notification': ['Push notification issue', 'Alert settings', 'Communication preferences']
                }
            },
            'Phone': {
                'types': ['Product Inquiry', 'Order Issue', 'Complaint', 'General Support', 'Account Help'],
                'details': {
                    'Product Inquiry': ['Product details', 'Availability check', 'Price inquiry', 'Specifications'],
                    'Order Issue': ['Order status', 'Delivery delay', 'Missing item', 'Wrong item received'],
                    'Complaint': ['Service complaint', 'Product quality', 'Delivery issue', 'Staff behavior'],
                    'General Support': ['Product guidance', 'Service information', 'Policy clarification'],
                    'Account Help': ['Account access', 'Payment verification', 'Account security']
                }
            },
            'Email': {
                'types': ['Query', 'Feedback', 'Complaint', 'Support Request', 'Information Update'],
                'details': {
                    'Query': ['Product inquiry', 'Order status', 'Shipping query', 'Return policy'],
                    'Feedback': ['Product feedback', 'Service feedback', 'Website feedback'],
                    'Complaint': ['Product complaint', 'Service complaint', 'Delivery complaint'],
                    'Support Request': ['Technical support', 'Account support', 'Order support'],
                    'Information Update': ['Account update', 'Order modification', 'Shipping update']
                }
            },
            'Chat': {
                'types': ['Quick Query', 'Technical Support', 'Order Support', 'Product Support', 'Account Support'],
                'details': {
                    'Quick Query': ['Product availability', 'Price check', 'Delivery time', 'Return policy'],
                    'Technical Support': ['Website issues', 'Payment problems', 'Login help', 'Checkout assistance'],
                    'Order Support': ['Order tracking', 'Modification request', 'Cancellation request'],
                    'Product Support': ['Product details', 'Usage guidance', 'Compatibility check'],
                    'Account Support': ['Password reset', 'Account access', 'Profile update']
                }
            },
            'Social Media': {
                'types': ['Comment', 'Direct Message', 'Review', 'Query', 'Complaint'],
                'details': {
                    'Comment': ['Product comment', 'Service comment', 'General feedback'],
                    'Direct Message': ['Product inquiry', 'Order status', 'Support request'],
                    'Review': ['Product review', 'Service review', 'Overall experience'],
                    'Query': ['Product query', 'Service query', 'Availability check'],
                    'Complaint': ['Product complaint', 'Service complaint', 'Delivery complaint']
                }
            }
        }

        # Select interaction type and detail based on channel
        channel_name = channel['channel_name']
        interaction_type = np.random.choice(channel_interactions[channel_name]['types'])
        interaction_detail = np.random.choice(channel_interactions[channel_name]['details'][interaction_type])

        # Generate resolution status (weighted towards resolved)
        resolution_statuses = ['Resolved', 'Pending', 'In Progress', 'Escalated', 'Closed']
        resolution_status = np.random.choice(resolution_statuses, p=[0.6, 0.1, 0.15, 0.05, 0.1])

        # Generate duration based on channel and interaction type
        if channel['channel_type'] == 'Digital':
            # Digital channels typically have shorter durations
            duration = np.random.randint(30, 900)  # 30 seconds to 15 minutes
        else:
            # Traditional channels like phone typically take longer
            duration = np.random.randint(120, 1800)  # 2 minutes to 30 minutes

        # Create interaction event
        interaction = {
            'interaction_id': self.interaction_id,
            'customer_id': int(customer['customer_id']),
            'date_id': datetime.now().strftime('%Y%m%d'),
            'channel_id': int(channel['channel_id']),
            'interaction_type': interaction_type,
            'interaction_detail': f"{interaction_detail}",
            'resolution_status': resolution_status,
            'duration_seconds': duration,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        self.interaction_id += 1
        return interaction

    def generate_batch_interaction(self, batch_size) -> List[Dict]:
        # Generate a batch of sale events
        return [self.generate_single_interaction() for _ in range(batch_size)]

    def send_interaction_events(self, topic, batch_size):
        # Send batch of sales events to Kafka topic
        interaction_batch = self.generate_batch_interaction(batch_size)

        # Send each sale event to Kafka
        try:
            success = send_to_kafka(
                self.producer,
                KAFKA_TOPICS[topic],
                interaction_batch,
                batch=True
            )
            if success:
                print(f"Sent {batch_size} interaction events at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                logger.error("Failed to send interaction batch")
        except Exception as e:
            logger.error(f"Error in send_interaction_events: {str(e)}")

    def start_streaming(self, interval, batch_size):
        # Start streaming sales events at specified interval
        try:
            print(f"Starting interaction event streaming to Kafka...")
            print(f"Batch size: {batch_size} events every {interval} seconds")
            while True:
                self.send_interaction_events('customer_interaction', batch_size)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nStopping interaction event streaming...")
            close_producer(self.producer)
            print("Producer closed successfully")
        except Exception as e:
            print(f"Failed closing Kafka producer because of error {str(e)}")
            logger.error(e)


if __name__ == "__main__":
    # Configure Kafka producer settings
    BATCH_SIZE = 1000
    INTERVAL_SECONDS = 3

    # Create and start producer
    try:
        producer = ProductReviewProducer()
        producer.start_streaming(
            interval=INTERVAL_SECONDS,
            batch_size=BATCH_SIZE
        )
    except Exception as e:
        print(f"Main Execution Error: {str(e)}")

