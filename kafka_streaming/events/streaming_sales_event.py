from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from kafka_streaming.utils.kafka_utils import *

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPICS = 'sales_events'
KAFKA_BROKERS = 'broker:29092'
NUM_PARTITIONS = 10
REPLICATION_FACTOR = 1


# Logging
logger = setup_logger()

# File path configuration
customers = pd.read_csv(customers_file_path)
products = pd.read_csv(products_file_path)
locations = pd.read_csv(locations_file_path)
websites = pd.read_csv(website_file_path)
navigation = pd.read_csv(navigation_file_path)
warehouses = pd.read_csv(warehouse_file_path)
promotions = pd.read_csv(promotion_file_path)
date = pd.read_csv(date_file_path)
time_element = pd.read_csv(time_file_path)
ship_modes = pd.read_csv(ship_mode_file_path)


# Load dimension data
def load_dimension_data():
    dimensions = {
        'customers': 'customer_dimension.csv',
        'products': 'product_dimension.csv',
        'locations': 'location_dimension.csv',
        'websites': 'website_dimension.csv',
        'navigation': 'navigation_dimension.csv',
        'warehouses': 'warehouse_dimension.csv',
        'promotions': 'promotion_dimension.csv',
        'date': 'date_dimension.csv',
        'time_element': 'time_dimension.csv',
        'ship_modes': 'ship_mode_dimension.csv'
    }

    return {name: pd.read_csv(os.path.join(data_dir, filename)) for name, filename in dimensions.items()}


def generate_single_sale(dimensions):
    """Generate a single sales event"""
    product = dimensions['products'].sample(n=1).iloc[0]

    list_price = round(np.random.uniform(
        product.get("min_price", 50),
        product.get("max_price", 600)
    ), 2)

    discount_percentage = np.random.uniform(0.05, 0.3)
    discount_amount = round(list_price * discount_percentage, 2)
    sale_price = round(list_price - discount_amount, 2)

    return {
        "product_key": int(product["product_key"]),
        "time_key": int(np.random.choice(dimensions['time_element']["time_key"])),
        "date_key": int(np.random.choice(dimensions['date']["date_key"])),
        "warehouse_key": int(np.random.choice(dimensions['warehouses']["warehouse_key"])),
        "promotion_key": int(np.random.choice(dimensions['promotions']["promotion_key"])),
        "customer_key": int(np.random.choice(dimensions['customers']["customer_key"])),
        "location_key": int(np.random.choice(dimensions['locations']["location_key"])),
        "website_key": int(np.random.choice(dimensions['websites']["website_key"])),
        "navigation_key": int(np.random.choice(dimensions['navigation']["navigation_key"])),
        "ship_mode_key": int(np.random.choice(dimensions['ship_modes']["ship_mode_key"])),
        "total_order_quantity": int(np.random.randint(1, 10)),
        "line_item_discount_amount": discount_amount,
        "line_item_sale_amount": sale_price,
        "line_item_list_price": list_price,
        "average_line_item_sale": round(np.random.uniform(sale_price * 0.95, sale_price * 1.05), 2),
        "average_line_item_list_price": round(np.random.uniform(list_price * 0.95, list_price * 1.05), 2)
    }


class KafkaSalesStreamer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='sales_events'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type="gzip"
        )
        self.topic = topic
        self.dimensions = load_dimension_data()
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logging.basicConfig(
            filename="../log.txt",
            level=logging.INFO,
            format='%(asctime)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def generate_sales_batch(self, batch_size):
        return [generate_single_sale(self.dimensions) for _ in range(batch_size)]

    def send_sales_events(self, batch_size):
        try:
            sales_batch = self.generate_sales_batch(batch_size)
            for sale in sales_batch:
                self.producer.send(self.topic, value=sale)

            self.producer.flush()
            print(f"Sent {batch_size} sales events at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            self.logger.error(f"Error sending sales events: {e}")

    def start_streaming(self, batch_size, interval):
        try:
            print(f"Starting sales event streaming to Kafka...")
            print(f"Batch size: {batch_size} events every {interval} seconds")

            while True:
                self.send_sales_events(batch_size)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nStopping sales event streaming...")
        finally:
            self.producer.close()


if __name__ == "__main__":
    streamer = KafkaSalesStreamer()
    # Configure Kafka producer settings
    BATCH_SIZE = 20000
    INTERVAL_SECONDS = 10  # 20000 messages every 10 secs

    try:
        streamer.start_streaming(
            interval=INTERVAL_SECONDS,
            batch_size=BATCH_SIZE
        )
    except Exception as e:
        print(f"Main Execution Error: {str(e)}")
