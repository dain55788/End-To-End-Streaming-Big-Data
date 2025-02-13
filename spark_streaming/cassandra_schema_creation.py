# DIMENSIONAL TABLES
def create_warehouse_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.warehouse_dimension (
        warehouse_key INT PRIMARY KEY,
        warehouse_name TEXT,
        warehouse_manager TEXT,
        warehouse_type TINYTEXT,
        warehouse_address TEXT,
        warehouse_city TINYTEXT,
        warehouse_state TINYTEXT,
        warehouse_country TINYTEXT,
        warehouse_postal_code TINYTEXT,
        warehouse_staff_count INT,
        warehouse_space_used INT
    );
    """)
    print("Warehouse dimension table created successfully!")


def create_location_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.location_dimension (
        location_key INT PRIMARY KEY,
        location_name TEXT,
        address TEXT,
        city TINYTEXT,
        state TINYTEXT,
        source TINYTEXT,
        country TINYTEXT,
        region TINYTEXT,
        international_code TINYTEXT,
        postal_code TINYTEXT
    );
    """)
    print("Location dimension table created successfully!")


def create_product_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.product_dimension (
        product_key INT PRIMARY KEY,
        SKU_number TINYTEXT,
        product_description TEXT,
        package_type TINYTEXT,
        package_size TINYTEXT,
        size TINYTEXT,
        color TINYTEXT,
        brand TINYTEXT,
        category TINYTEXT,
        subcategory TINYTEXT,
        department TINYTEXT,
        manufacturer TEXT,
        weight INT,
        unit_of_measure TINYTEXT,
        unit_price_base INT
    );
    """)
    print("Product dimension table created successfully!")


def create_promotion_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.promotion_dimension (
        promotion_key INT PRIMARY KEY,
        promotion_name TEXT,
        promotion_type TINYTEXT,
        promotion_channel TINYTEXT,
        promotion_desc TEXT,
        display_type TINYTEXT,
        display_location TEXT,
        display_provider TEXT,
        promo_cost DOUBLE,
        promo_begin_date DATE,
        promo_end_date DATE
    );
    """)
    print("Promotion dimension table created successfully!")


def create_website_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.website_dimension (
        website_key INT PRIMARY KEY,
        website_name TEXT,
        website_url TEXT,
        website_type TINYTEXT,
        website_category TINYTEXT,
        website_description TEXT,
        navigation_urls TEXT,
        banner_name TEXT,
        tool_bar TINYTEXT,
        number_of_products_per_page INT,
        search_available TINYTEXT,
        site_placement TINYTEXT
    );
    """)
    print("Website dimension table created successfully!")


def create_time_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.time_dimension (
        time_key INT PRIMARY KEY,
        time_desc TINYTEXT,
        hour_24 INT,
        am_pm_flag TINYTEXT,
        morning_flag TINYTEXT,
        afternoon_flag TINYTEXT,
        evening_flag TINYTEXT,
        late_evening_flag TINYTEXT
    );
    """)
    print("Time dimension table created successfully!")


def create_customer_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.customer_dimension (
        customer_key INT PRIMARY KEY,
        cust_first_name TINYTEXT,
        cust_last_name TINYTEXT,
        cust_full_name TEXT,
        cust_birth_date DATE,
        cust_gender TINYTEXT,
        cust_title TINYTEXT,
        contact_phone_number TINYTEXT,
        cust_email_address TEXT,
        cust_phone TINYTEXT,
        cust_address TEXT,
        cust_city TINYTEXT,
        cust_state TINYTEXT,
        cust_country TINYTEXT,
        cust_zipcode TINYTEXT,
        cust_primary_language TINYTEXT,
        cust_primary_postal_code TINYTEXT
    );
    """)
    print("Customer dimension table created successfully!")


def create_navigation_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.navigation_dimension (
        navigation_key INT PRIMARY KEY,
        from_the_url TEXT,
        to_the_url TEXT,
        referer_url TEXT,
        landing_url TEXT,
        exit_url TEXT,
        search_url TEXT,
        signin_url TEXT,
        signup_url TEXT,
        checkout_url TEXT,
        payment_url TEXT,
        floorplan_url TEXT,
        forum_url TEXT,
        blog_url TEXT,
        social_media_access_flag TINYTEXT,
        page_access_flag TINYTEXT,
        page_description TEXT
    );
    """)
    print("Navigation dimension table created successfully!")


def create_ship_mode_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.ship_mode_dimension (
        ship_mode_key INT PRIMARY KEY,
        ship_mode_name TINYTEXT,
        ship_mode_type TINYTEXT,
        carrier_name TEXT,
        carrier_city TINYTEXT,
        carrier_country TINYTEXT,
        carrier_primary_postal_code TINYTEXT,
        contact_phone_number TINYTEXT
    );
    """)
    print("Ship mode dimension table created successfully!")


def create_date_dimension(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.date_dimension (
        date_key INT PRIMARY KEY,
        calendar_date DATE,
        date_desc TEXT,
        calendar_month_number INT,
        day_number_overall INT,
        day_of_week TINYTEXT,
        day_of_week_number INT,
        week_number INT,
        week_desc_current INT,
        month TINYTEXT,
        month_number_current INT,
        fiscal_year TINYTEXT,
        fiscal_year_month TINYTEXT,
        holiday TEXT,
        holiday_flag TINYTEXT,
        special_day TEXT,
        season TINYTEXT,
        event TINYTEXT
    );
    """)
    print("Date dimension table created successfully!")


def create_all_dimension_tables(session):
    create_customer_dimension(session)
    create_navigation_dimension(session)
    create_ship_mode_dimension(session)
    create_date_dimension(session)
    create_warehouse_dimension(session)
    create_location_dimension(session)
    create_product_dimension(session)
    create_promotion_dimension(session)
    create_website_dimension(session)
    create_time_dimension(session)
    print("All dimension tables created successfully!")


# FACT TABLE
def create_sales_fact_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.sales_fact (
        product_key INT,
        time_key INT,
        date_key INT,
        warehouse_key INT,
        promotion_key INT,
        customer_key INT,
        location_key INT,
        website_key INT,
        navigation_key INT,
        ship_mode_key INT,
        total_order_quantity INT,
        line_item_discount_amount DOUBLE,
        line_item_sale_amount DOUBLE,
        line_item_list_price DOUBLE,
        average_line_item_sale DOUBLE,
        average_line_item_list_price DOUBLE,
        PRIMARY KEY ((product_key), time_key)
    ));
    """)

    print("Sales Fact Table created successfully!")
