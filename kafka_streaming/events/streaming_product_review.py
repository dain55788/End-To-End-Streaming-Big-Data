import pandas as pd
import numpy as np
from datetime import datetime
import time
from typing import Dict, List
from kafka_utils import *

# logger basic file core:
logger = setup_logger()


class CustomerInteractionProducer:
    def __init__(self):
        # Initialize Kafka producer (from Kafka utils)
        self.producer = create_kafka_producer()

        # Load dimension data
        self.customers = pd.read_csv(customers_file_path)
        self.products = pd.read_csv(products_file_path)
        self.channels = pd.read_csv(channel_file_path)
        self.date = pd.read_csv(date_file_path)

        # Initialize sale_id counter
        self.review_id = 1

    def generate_single_review(self) -> Dict:
        """Generate a single product review event"""
        # Select random product and customer
        product = self.products.iloc[np.random.randint(len(self.products))]
        customer = self.customers.iloc[np.random.randint(len(self.customers))]

        # Generate rating (weighted towards positive reviews)
        rating = np.random.choice([1, 2, 3, 4, 5], p=[0.05, 0.10, 0.15, 0.30, 0.40])

        # Generate review text based on rating
        review_templates = {
            5: [
                f"Absolutely love this {product['category']}! {product['product_name']} exceeded my expectations. The quality is outstanding.",
                f"Best {product['category']} I've ever purchased. Worth every penny.",
                f"This {product['product_name']} is perfect! Exactly what I was looking for.",
                f"Five stars! Superior quality and great value for money.",
                f"Extremely satisfied with this purchase. Would highly recommend!"
            ],
            4: [
                f"Really good {product['category']}. {product['product_name']} works great.",
                f"Very satisfied with this purchase. Minor room for improvement.",
                f"Good quality product, delivers what it promises.",
                f"Nice {product['product_name']}, would buy again.",
                f"Solid choice, just a few minor issues."
            ],
            3: [
                f"Decent {product['category']}. Does the job but nothing special.",
                f"Average quality. Expected a bit more for the price.",
                f"It's okay. Has both pros and cons.",
                f"Middle of the road {product['product_name']}.",
                f"Not bad, but not great either."
            ],
            2: [
                f"Disappointed with this {product['category']}. Below expectations.",
                f"Several issues with the {product['product_name']}. Would not recommend.",
                f"Not worth the price. Quality could be better.",
                f"Mediocre product with some significant flaws.",
                f"Expected much better quality."
            ],
            1: [
                f"Very poor quality. Would not recommend this {product['category']}.",
                f"Complete waste of money. Avoid this {product['product_name']}!",
                f"Terrible product. Many issues and problems.",
                f"Don't buy this. Save your money.",
                f"Extremely disappointed. Would give zero stars if possible."
            ]
        }

        review_text = np.random.choice(review_templates[rating])

        # Generate helpful votes (higher rated products tend to get more helpful votes)
        helpful_votes = np.random.randint(0, int(rating * 20))

        # 80% chance of being a verified purchase
        is_verified_purchase = np.random.choice([0, 1], p=[0.2, 0.8])

        # Create review event
        review = {
            'review_id': self.review_id,
            'product_id': int(product['product_id']),
            'customer_id': int(customer['customer_id']),
            'date_id': datetime.now().strftime('%Y%m%d'),
            'rating': int(rating),
            'review_text': review_text,
            'helpful_votes': int(helpful_votes),
            'is_verified_purchase': int(is_verified_purchase),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        self.review_id += 1
        return review

    def generate_batch_reviews(self, batch_size) -> List[Dict]:
        # Generate a batch of sale events
        return [self.generate_single_review() for _ in range(batch_size)]

    def send_reviews_events(self, topic, batch_size):
        # Send batch of reviews events to Kafka topic
        reviews_batch = self.generate_batch_reviews(batch_size)

        # Send each sale event to Kafka
        try:
            success = send_to_kafka(
                self.producer,
                KAFKA_TOPICS[topic],
                reviews_batch,
                batch=True
            )
            if success:
                print(f"Sent {batch_size} reviews events at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                logger.error("Failed to send reviews batch")
        except Exception as e:
            logger.error(f"Error in send_reviews_events: {str(e)}")

    def start_streaming(self, interval, batch_size):
        # Start streaming reviews events at specified interval
        try:
            print(f"Starting reviews event streaming to Kafka...")
            print(f"Batch size: {batch_size} events every {interval} seconds")
            while True:
                self.send_reviews_events('product_review', batch_size)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nStopping reviews event streaming...")
            close_producer(self.producer)
            print("Producer closed successfully")
        except Exception as e:
            print(f"Failed closing Kafka producer because of error {str(e)}")
            logger.error(e)


if __name__ == "__main__":
    # Configure Kafka producer settings
    BATCH_SIZE = 10  # Modify back to 100
    INTERVAL_SECONDS = 3

    # Create and start producer
    try:
        producer = CustomerInteractionProducer()
        producer.start_streaming(
            interval=INTERVAL_SECONDS,
            batch_size=BATCH_SIZE
        )
    except Exception as e:
        print(f"Main Execution Error: {str(e)}")

