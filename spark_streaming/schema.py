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
    StructField("customer_key", IntegerType(), False),  # Primary key
    StructField("cust_first_name", StringType(), False),
    StructField("cust_last_name", StringType(), False),
    StructField("cust_full_name", StringType(), False),
    StructField("cust_birth_day", DateType(), True),
    StructField("cust_gender", StringType(), True),
    StructField("cust_title", StringType(), True),
    StructField("contact_phone_number", StringType(), False),
    StructField("cust_email_address", StringType(), False),
    StructField("cust_phone", StringType(), False),
    StructField("cust_address", StringType(), False),
    StructField("cust_state", StringType(), True),
    StructField("cust_city", StringType(), True),
    StructField("cust_country", StringType(), True),
    StructField("cust_zipcode", StringType(), False),
    StructField("cust_primary_language", StringType(), True),
    StructField("cust_primary_postal_code", StringType(), False),
])

dim_product = StructType([
   StructField("product_key", IntegerType(), False),
   StructField("sku_number", StringType(), False),
   StructField("product_description", StringType(), False),
   StructField("package_type", StringType(), False),
   StructField("package_size", StringType(), False),
   StructField("size", StringType(), False),
   StructField("color", StringType(), False),
   StructField("brand", StringType(), False),
   StructField("category", StringType(), False),
   StructField("subcategory", StringType(), False),
   StructField("department", StringType(), False),
   StructField("manufacturer", StringType(), False),
   StructField("weight", IntegerType(), False),
   StructField("unit_of_measure", StringType(), False),
   StructField("unit_price_base", IntegerType(), False)
])

dim_location = StructType([
    StructField("location_key", IntegerType(), False),  # Primary key
    StructField("location_name", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("source", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("international_code", StringType(), False),
    StructField("postal_code", StringType(), False),
])

dim_warehouse = StructType([
    StructField("warehouse_key", IntegerType(), False),
    StructField("warehouse_name", StringType(), False),
    StructField("warehouse_manager", StringType(), True),
    StructField("warehouse_address", StringType(), False),
    StructField("warehouse_city", StringType(), True),
    StructField("warehouse_state", StringType(), True),
    StructField("warehouse_country", StringType(), True),
    StructField("warehouse_postal_code", StringType(), False),
    StructField("warehouse_staff_count", IntegerType(), True),
    StructField("warehouse_space_used", IntegerType(), True)
])

dim_promotion = StructType([
    StructField("promotion_key", IntegerType(), False),
    StructField("promotion_name", StringType(), False),
    StructField("promotion_type", StringType(), True),
    StructField("promotion_channel", StringType(), True),
    StructField("promotion_desc", StringType(), True),
    StructField("display_type", StringType(), False),
    StructField("display_location", StringType(), True),
    StructField("display_provider", StringType(), True),
    StructField("promo_cost", DoubleType(), False),
    StructField("promo_begin_date", DateType(), False),
    StructField("promo_end_date", DateType(), False)
])

dim_website = StructType([
    StructField("website_key", IntegerType(), False),
    StructField("website_name", StringType(), False),
    StructField("website_url", StringType(), False),
    StructField("website_type", StringType(), False),
    StructField("website_category", StringType(), False),
    StructField("website_description", StringType(), True),
    StructField("navigation_urls", StringType(), False),
    StructField("banner_name", StringType(), True),
    StructField("tool_bar", StringType(), True),
    StructField("number_of_products_per_page", IntegerType(), False),
    StructField("search_available", StringType(), True),
    StructField("site_placement", StringType(), True)
])

dim_navigation = StructType([
   StructField("navigation_key", IntegerType(), False),
   StructField("from_the_url", StringType(), False),
   StructField("to_the_url", StringType(), False),
   StructField("refer_url", StringType(), False),
   StructField("landing_url", StringType(), False),
   StructField("exit_url", StringType(), False),
   StructField("search_url", StringType(), False),
   StructField("signin_url", StringType(), False),
   StructField("signup_url", StringType(), False),
   StructField("checkout_url", StringType(), False),
   StructField("payment_url", StringType(), False),
   StructField("floorplan_url", StringType(), False),
   StructField("forum_url", StringType(), False),
   StructField("blog_url", StringType(), False),
   StructField("social_media_access_flag", StringType(), False),
   StructField("page_access_flag", StringType(), False),
   StructField("page_description", StringType(), True)
])

dim_ship_mode = StructType([
   StructField("ship_mode_key", IntegerType(), False),
   StructField("ship_mode_name", StringType(), False),
   StructField("ship_mode_type", StringType(), False),
   StructField("carrier_name", StringType(), False),
   StructField("carrier_id", StringType(), False),
   StructField("carrier_country", StringType(), False),
   StructField("carrier_primary_postal_code", StringType(), False),
   StructField("contact_phone_number", StringType(), False)
])

dim_date = StructType([
   StructField("date_key", IntegerType(), False),
   StructField("calendar_date", DateType(), False),
   StructField("calendar_month_number", IntegerType(), False),
   StructField("day_number_overall", IntegerType(), False),
   StructField("day_of_week", StringType(), False),
   StructField("day_of_week_number", IntegerType(), False),
   StructField("week_number", IntegerType(), False),
   StructField("week_desc_current", IntegerType(), False),
   StructField("month", StringType(), False),
   StructField("month_number_current", IntegerType(), False),
   StructField("fiscal_year", StringType(), False),
   StructField("fiscal_year_month", StringType(), False),
   StructField("holiday", StringType(), False),
   StructField("holiday_flag", StringType(), False),
   StructField("special_day", StringType(), False),
   StructField("season", StringType(), False),
   StructField("event", StringType(), False)
])

dim_time = StructType([
   StructField("time_key", IntegerType(), False),
   StructField("time_desc", StringType(), False),
   StructField("hour_24", IntegerType(), False),
   StructField("am_pm_flag", StringType(), False),
   StructField("morning_flag", StringType(), False),
   StructField("afternoon_flag", StringType(), False),
   StructField("evening_flag", StringType(), False),
   StructField("late_evening_flag", StringType(), False)
])

# Fact Tables Schemas
sales_fact_table = StructType([
    StructField("product_key", IntegerType(), False),
    StructField("time_key", IntegerType(), False),
    StructField("date_key", IntegerType(), False),
    StructField("warehouse_key", IntegerType(), False),
    StructField("promotion_key", IntegerType(), False),
    StructField("customer_key", IntegerType(), False),
    StructField("location_key", IntegerType(), False),
    StructField("website_key", IntegerType(), False),
    StructField("navigation_key", IntegerType(), False),
    StructField("ship_mode_key", IntegerType(), False),
    StructField("total_order_quantity", IntegerType(), False),
    StructField("line_item_discount_amount", DoubleType(), False),
    StructField("line_item_sale_amount", DoubleType(), False),
    StructField("line_item_list_price", DoubleType(), False),
    StructField("average_line_item_sale", DoubleType(), False),
    StructField("average_line_item_list_price", DoubleType(), False)
])

table_schemas = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_location": dim_location,
    "dim_warehouse": dim_warehouse,
    "dim_promotion": dim_promotion,
    "dim_website": dim_website,
    "dim_navigation": dim_navigation,
    "dim_ship_mode": dim_ship_mode,
    "dim_date": dim_date,
    "dim_time": dim_time,
    "sales_fact": sales_fact_table
}


# helper function to get schema table:
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
