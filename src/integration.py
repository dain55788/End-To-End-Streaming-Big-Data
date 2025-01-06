from src.hiveconnect.hiveapp import *
from src.hiveconnect import hiveapp
username = 'nguyendai'


# All in one setting up function
def setup_warehouse():
    try:
        # Create database first
        create_database('ecommerce_warehouse', username)

        # Create dimensional tables
        create_table_dim_customer(username)
        create_table_dim_product(username)
        create_table_dim_payment(username)
        create_table_dim_location(username)
        create_table_dim_channel(username)
        create_table_dim_date(username)

        # Create fact tables
        create_fact_sales(username)
        create_fact_product_review(username)
        create_fact_customer_interaction(username)

        logging.info("Data warehouse setup completed successfully")

    except Exception as e:
        logging.error(f"Failed to setup data warehouse: {e}")
        raise


#  Load all dimensional csv file into dimensional tables
def load_all_dimension_tables():
    """
    Load data for all dimensional tables
    """
    dimension_tables = {
        'dim_customer': 'dim_customer.csv',
        'dim_product': 'dim_product.csv',
        'dim_payment': 'dim_payment.csv',
        'dim_location': 'dim_location.csv',
        'dim_channel': 'dim_channel.csv',
        'dim_date': 'dim_date.csv'
    }

    for table_name, csv_file in dimension_tables.items():
        success = load_data(csv_file, table_name, username)
        if not success:
            logging.error(f"Failed to load {table_name}")

