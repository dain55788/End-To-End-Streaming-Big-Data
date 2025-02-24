from datetime import datetime
from kafka_streaming.utils.kafka_utils import *
import pandas as pd
import time

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'broker:29092'
KAFKA_TOPICS = 'amazon_sales_events'
NUM_PARTITIONS = 10
REPLICATION_FACTOR = 1


# Load Amazon sales data
def load_amazon_sales_data(file_path=AMAZON_SALES_DATA):
    try:
        df = pd.read_csv(file_path, low_memory=False)
        # # These will ensure the DataFrame has the expected columns (based on your Amazon Sales Report)
        # # For basic purpose, we won't need it
        # expected_columns = ['index', 'order_id', 'date', 'status', 'fulfillment', 'sales_channel',
        #                     'ship_service_level', 'style', 'sku', 'category', 'size', 'asin',
        #                     'courier_status', 'qty', 'currency', 'amount', 'ship_city',
        #                     'ship_state', 'ship_postal', 'ship_country', 'promotion_ids', 'B2B',
        #                     'fulfilled_by']
        # # Check if all expected columns exist
        # missing_columns = [col for col in expected_columns if col not in df.columns]
        # if missing_columns:
        #     raise ValueError(f"Missing columns in CSV: {missing_columns}")

        # convert DataFrame to list of dictionaries
        sales_data = df.to_dict(orient='records')
        return sales_data
    except Exception as e:
        logger.error(f"Error loading Amazon sales data: {e}")
        raise


class KafkaSalesStreamer:
    def __init__(self, topic=KAFKA_TOPICS):
        self.producer = create_kafka_producer()
        self.topic = topic
        self.sales_data = load_amazon_sales_data()
        self.current_index = 0
        self.logger = setup_logger()

    def send_sales_events(self, batch_size):
        try:
            # ensure we don't exceed the length of the data
            end_index = min(self.current_index + batch_size, len(self.sales_data))
            if self.current_index >= len(self.sales_data):
                raise StopIteration("No more data to send.")

            sales_batch = self.sales_data[self.current_index:end_index]
            send_to_kafka(self.producer, self.topic, sales_batch, batch=True)
            print(f"Sent {len(sales_batch)} Amazon sales events at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.current_index = end_index  # update the current position/ index
        except Exception as e:
            self.logger.error(f"Error sending sales events: {e}")

    def start_streaming(self, batch_size, interval):
        try:
            print(f"Starting Amazon sales event streaming to Kafka from CSV")
            print(f"Batch size: {batch_size} events every {interval} seconds")

            while True:
                self.send_sales_events(batch_size)
                time.sleep(interval)

        except StopIteration as sit:
            print("\nNo more data to stream. Closing producer and stopping streaming...")
            self.logger.error(f"Streaming error: {sit}")
        except KeyboardInterrupt as kb:
            print("\nStopping Amazon sales event streaming...")
            self.logger.error(f"Streaming error: {kb}")
        except Exception as e:
            print(f"Streaming error: {str(e)}")
            self.logger.error(f"Streaming error: {e}")
        finally:
            close_producer(self.producer)


# Producer
if __name__ == "__main__":
    streamer = KafkaSalesStreamer()
    # Configure Kafka producer settings
    BATCH_SIZE = 200
    INTERVAL_SECONDS = 5  # 200 messages every 5 secs

    try:
        streamer.start_streaming(
            interval=INTERVAL_SECONDS,
            batch_size=BATCH_SIZE
        )
    except Exception as e:
        print(f"Main Execution Error: {str(e)}")
