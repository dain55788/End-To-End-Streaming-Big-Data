from pyspark.sql.types import (IntegerType,
                               StringType,
                               DoubleType,
                               StructField,
                               StructType,
                               LongType,
                               BooleanType,
                               TimestampType,
                               DecimalType,
                               DateType)
from pyspark.sql.functions import *

# Dimensional Table schema
dim_customer = StructType([
    StructField("customer_id", IntegerType(), False),  # Primary key
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

dim_product = StructType([
    StructField("product_id", IntegerType(), False),  # Primary key
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("base_price", DecimalType(10, 2), True),
    StructField("supplier", StringType(), True),
    StructField("is_active", IntegerType(), True),
    StructField("valid_from", TimestampType(), True),
    StructField("created_at", TimestampType(), True)
])

dim_payment = StructType([
    StructField("payment_id", IntegerType(), False),  # Primary key
    StructField("method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("processing_fee", DecimalType(10, 2), True),
    StructField("created_at", TimestampType(), True)
])

dim_location = StructType([
    StructField("location_id", IntegerType(), False),  # Primary key
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("region", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

dim_channel = StructType([
    StructField("channel_id", IntegerType(), False),  # Primary key
    StructField("channel_name", StringType(), True),
    StructField("channel_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

dim_date = StructType([
    StructField("date_id", IntegerType(), False),  # Primary key
    StructField("full_date", DateType(), True),
    StructField("year", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("week", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True),
    StructField("is_holiday", IntegerType(), True),
    StructField("created_at", TimestampType(), True)
])

# Fact Tables Schemas
fact_sales = StructType([
    StructField("sale_id", IntegerType(), False),  # Primary key
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),  # Foreign key
    StructField("product_id", IntegerType(), True),  # Foreign key
    StructField("date_id", IntegerType(), True),     # Foreign key
    StructField("location_id", IntegerType(), True),  # Foreign key
    StructField("payment_id", IntegerType(), True),  # Foreign key
    StructField("order_datetime", TimestampType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("discount", DecimalType(10, 2), True),
    StructField("total_amount", DecimalType(10, 2), True),
    StructField("tax", DecimalType(10, 2), True),
    StructField("created_at", TimestampType(), True)
])

fact_product_review = StructType([
    StructField("review_id", IntegerType(), False),  # Primary key
    StructField("product_id", IntegerType(), True),  # Foreign key
    StructField("customer_id", IntegerType(), True),  # Foreign key
    StructField("date_id", IntegerType(), True),     # Foreign key
    StructField("rating", IntegerType(), True),
    StructField("review_text", StringType(), True),
    StructField("helpful_votes", IntegerType(), True),
    StructField("is_verified_purchase", IntegerType(), True),
    StructField("created_at", TimestampType(), True)
])

fact_customer_interaction = StructType([
    StructField("interaction_id", IntegerType(), False),  # Primary key
    StructField("customer_id", IntegerType(), True),      # Foreign key
    StructField("date_id", IntegerType(), True),          # Foreign key
    StructField("channel_id", IntegerType(), True),       # Foreign key
    StructField("interaction_type", StringType(), True),
    StructField("resolution_status", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("interaction_details", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

table_schemas = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_payment": dim_payment,
    "dim_location": dim_location,
    "dim_channel": dim_channel,
    "dim_date": dim_date,
    "fact_sales": fact_sales,
    "fact_product_review": fact_product_review,
    "fact_customer_interaction": fact_customer_interaction
}


# Helper function to get schema table:
def get_schema_table(table_name):
    """
     Get the Spark schema for a given table name

    Parameters:
        table_name: str - Name of the table

    Returns:
        StructType: Spark schema for the table

    Raises:
        KeyError: If table name is not found
    """
    schema = table_schemas.get(table_name)
    if not schema:
        raise KeyError("Schema not found in the table schema!!")
    return schema
