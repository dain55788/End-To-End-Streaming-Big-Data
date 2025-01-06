from pyhive import hive
import pyodbc
import logging
import os
import pandas as pd
# logging basic file core:-
logging.basicConfig(filename="log.txt", level=logging.DEBUG,
                    filemode='a',
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


def create_hive_connection(username):
    connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
    return connection


# to create database
def create_database(dbname, username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='default')
        cur = connection.cursor()
        cur.execute(f"CREATE DATABASE {dbname}")

        print(f"\nDatabase: - {dbname} is created successfully \n")
        logging.info(f"Database: - {dbname}is created successfully")

    except Exception as e:
        logging.error(e)


# create Rental Dimension
def create_table_dim_customer(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS dim_customer (
                    customer_id INT,
                    first_name STRING,
                    last_name STRING,
                    phone STRING,
                    email STRING,
                    segment STRING,
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Table dim_customer is created successfully \n")
        logging.info('Table:- dim_customer is created successfully')

    except Exception as e:
        logging.error(e)


def create_table_dim_product(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS dim_product (
                    product_id INT,
                    product_name STRING,
                    category STRING,
                    subcategory STRING,
                    brand STRING,
                    base_price DECIMAL(10,2),
                    supplier STRING,
                    is_active INT,
                    valid_from TIMESTAMP,
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Table dim_product is created successfully \n")
        logging.info('Table:- dim_product is created successfully')

    except Exception as e:
        logging.error(e)


def create_table_dim_location(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS dim_location (
                    location_id INT,
                    city STRING,
                    state STRING,
                    country STRING,
                    postal_code STRING,
                    timezone STRING,
                    region STRING,
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Table dim_location is created successfully \n")
        logging.info('Table:- dim_location is created successfully')

    except Exception as e:
        logging.error(e)


def create_table_dim_date(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS dim_date (
                    date_id INT,
                    full_date DATE,
                    year INT,
                    quarter INT,
                    month INT,
                    week INT,
                    day INT,
                    is_weekend INT,
                    is_holiday INT,
                    created_at TIMESTAMP
                )
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Table dim_date is created successfully \n")
        logging.info('Table:- dim_date is created successfully')

    except Exception as e:
        logging.error(e)


def create_table_dim_channel(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS dim_channel (
                    channel_id INT,
                    channel_name STRING,
                    channel_type STRING,
                    description STRING,
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Table dim_channel is created successfully \n")
        logging.info('Table:- dim_channel is created successfully')

    except Exception as e:
        logging.error(e)


def create_table_dim_payment(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS dim_payment (
                    payment_id INT,
                    method STRING,
                    status STRING,
                    provider STRING,
                    processing_fee DECIMAL(10,2),
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Table dim_payment is created successfully \n")
        logging.info('Table:- dim_payment is created successfully')

    except Exception as e:
        logging.error(e)


def create_fact_sales(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS fact_sales (
                    sale_id INT,
                    order_id INT,
                    customer_id INT,
                    product_id INT,
                    date_id INT,
                    location_id INT,
                    payment_id INT,
                    order_datetime TIMESTAMP,
                    quantity INT,
                    unit_price DECIMAL(10,2),
                    discount DECIMAL(10,2),
                    total_amount DECIMAL(10,2),
                    tax DECIMAL(10,2),
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                CLUSTERED BY (customer_id) INTO 8 BUCKETS
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Fact table fact_sales is created successfully \n")
        logging.info('Table:- fact_sales is created successfully')

    except Exception as e:
        logging.error(e)


def create_fact_product_review(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS fact_product_review (
                    review_id INT,
                    product_id INT,
                    customer_id INT,
                    date_id INT,
                    rating INT,
                    review_text STRING,
                    helpful_votes INT,
                    is_verified_purchase INT,
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                CLUSTERED BY (product_id) INTO 4 BUCKETS
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Fact table product_review is integrated successfully \n")
        logging.info('Table:- product_review is integrated successfully')

    except Exception as e:
        logging.error(e)


def create_fact_customer_interaction(username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS fact_customer_interaction (
                    interaction_id INT,
                    customer_id INT,
                    date_id INT,
                    channel_id INT,
                    interaction_type STRING,
                    resolution_status STRING,
                    duration_seconds INT,
                    interaction_details STRING,
                    created_at TIMESTAMP
                )
                PARTITIONED BY (year INT, month INT, day INT)
                CLUSTERED BY (customer_id) INTO 4 BUCKETS
                STORED AS ORC
                TBLPROPERTIES ("orc.compress"="SNAPPY")''')

        print(f"Fact table customer_interaction is integrated successfully \n")
        logging.info('Table:- customer_interaction is integrated successfully')

    except Exception as e:
        logging.error(e)


# to load data into csv table
def load_data(csv_file_path, table_name, username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(project_root, 'data', csv_file_path)
        csv_path = csv_path.replace("\\", "/")

        load_data_sql = f""" 
                        LOAD DATA LOCAL INPATH '/{csv_path}'
                        INTO TABLE {table_name}
                        """
        # Execute SQL statement
        cursor = connection.cursor()
        cursor.execute(load_data_sql)
        cursor.close()
        connection.close()
        print(f"Data loaded into {table_name} successfully.")
        return True
    except Exception as e:
        logging.error(e)
        return False


# drop table temp table;
def drop_table(table_name, username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        cur = connection.cursor()

        cur.execute(f"DROP TABLE {table_name}")

        print(f'\n Table: {table_name} is deleted successfully \n')
        logging.info(f'Table: {table_name} is deleted successfully')

    except Exception as e:
        logging.error(e)

    # store the row details into python list of tuples


def extract_row(query, username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')

        df = pd.read_sql(query, connection)

        records = df.to_records(index=False)
        result = list(records)
        print('\n converting the rows_data into python list of tuples \n')
        print('converted suscessfully the rows_data into python list of tuples')
        return result
    except Exception as e:
        logging.error(e)
        return None


def df_rows_details(query, username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='ecommerce_warehouse')
        set_join = f"""
        set hive.auto.convert.join=false
        """
        # Execute SQL command
        cursor = connection.cursor()
        cursor.execute(set_join)
        df = pd.read_sql(query, connection)
        print('\n converting the rows_data into DataFrame \n')
        print('converted successfully the rows_data into DataFrame')
        print(df)

    except Exception as e:
        logging.error(e)
        return None

    return df

