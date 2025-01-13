-- Date Dimension
CREATE TABLE Date_Dimension (
    date_key INT PRIMARY KEY,
    calendar_date DATE,
    date_desc TEXT(100),
    calendar_month_number INT,
    day_number_overall INT,
    day_of_week TEXT(15),
    day_of_week_number INT,
    week_number INT,
    week_desc_current INT,
    month TEXT(15),
    month_number_current INT,
    fiscal_year TEXT(15),
    fiscal_year_month TEXT(15),
    holiday TEXT(100),
    holiday_flag TEXT(1),
    special_day TEXT(100),
    season TEXT(20),
    event TEXT(50)
);

-- Time Dimension
CREATE TABLE Time_Dimension (
    time_key INT PRIMARY KEY,
    time_desc TEXT(50),
    hour_24 INT,
    am_pm_flag TEXT(2),
    morning_flag TEXT(1),
    afternoon_flag TEXT(1),
    evening_flag TEXT(1),
    late_evening_flag TEXT(1)
);

-- Website Dimension
CREATE TABLE Website_Dimension (
    website_key INT PRIMARY KEY,
    website_name TEXT(100),
    website_url TEXT(200),
    website_type TEXT(50),
    website_category TEXT(50),
    website_description TEXT(500),
    navigation_urls TEXT(500),
    banner_name TEXT(100),
    tool_bar TEXT(50),
    number_of_products_per_page INT,
    search_available TEXT(1),
    site_placement TEXT(50)
);

-- Navigation Dimension
CREATE TABLE Navigation_Dimension (
    navigation_key INT PRIMARY KEY,
    from_the_url TEXT(200),
    to_the_url TEXT(200),
    referer_url TEXT(200),
    landing_url TEXT(200),
    exit_url TEXT(200),
    search_url TEXT(200),
    signin_url TEXT(200),
    signup_url TEXT(200),
    checkout_url TEXT(200),
    payment_url TEXT(200),
    floorplan_url TEXT(200),
    forum_url TEXT(200),
    blog_url TEXT(200),
    social_media_access_flag TEXT(1),
    page_access_flag TEXT(1),
    page_description TEXT(200)
);

-- Customer Dimension
CREATE TABLE Customer_Dimension (
    customer_key INT PRIMARY KEY,
    cust_first_name TEXT(50),
    cust_last_name TEXT(50),
    cust_full_name TEXT(100),
    cust_birth_date DATE,
    cust_gender TEXT(1),
    cust_title TEXT(10),
	contact_phone_number TEXT(20),
    cust_email_address TEXT(100),
    cust_phone TEXT(20),
    cust_address TEXT(200),
    cust_city TEXT(50),
    cust_state TEXT(50),
    cust_country TEXT(50),
    cust_zipcode TEXT(10),
    cust_primary_language TEXT(50),
    cust_primary_postal_code TEXT(10)
);

-- Warehouse Dimension
CREATE TABLE Warehouse_Dimension (
    warehouse_key INT PRIMARY KEY,
    warehouse_name TEXT(100),
    warehouse_manager TEXT(100),
    warehouse_type TEXT(50),
    warehouse_address TEXT(200),
    warehouse_city TEXT(50),
    warehouse_state TEXT(50),
    warehouse_country TEXT(50),
    warehouse_postal_code TEXT(10),
    warehouse_staff_count INT,
    warehouse_space_used INT
);

-- Product Dimension
CREATE TABLE Product_Dimension (
    product_key INT PRIMARY KEY,
    SKU_number TEXT(50),
    product_description TEXT(500),
    package_type TEXT(50),
    package_size TEXT(50),
    size TEXT(20),
    color TEXT(20),
    brand TEXT(20),
    category TEXT(50),
    subcategory TEXT(50),
    department TEXT(20),
    manufacturer TEXT(100),
    weight INT,
    unit_of_measure TEXT(20),
    unit_price_base INT
);

-- Promotion Dimension
CREATE TABLE Promotion_Dimension (
    promotion_key INT PRIMARY KEY,
    promotion_name TEXT(100),
    promotion_type TEXT(50),
    promotion_channel TEXT(50),
    promotion_desc TEXT(500),
    display_type TEXT(50),
    display_location TEXT(100),
    display_provider TEXT(100),
    promo_cost DOUBLE,
    promo_begin_date DATE,
    promo_end_date DATE
);

-- Location Dimension
CREATE TABLE Location_Dimension (
    location_key INT PRIMARY KEY,
    location_name TEXT(100),
    address TEXT(200),
    city TEXT(50),
    state TEXT(50),
    source TEXT(50),
    country TEXT(50),
    region TEXT(50),
    international_code TEXT(20),
    postal_code TEXT(10)
);

-- Ship mode Dimension
CREATE TABLE Ship_Mode_Dimension(
	ship_mode_key INT PRIMARY KEY,
    ship_mode_name TEXT(50),
    ship_mode_type TEXT(50),
    carrier_name TEXT(80),
    carrier_city TEXT(50),
    carrier_country TEXT(50),
    carrier_primary_postal_code TEXT(5),
    contact_phone_number TEXT(10)
);

-- Sales Fact Table
CREATE TABLE Sales_Fact_Table (
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
    FOREIGN KEY (product_key) REFERENCES Product_Dimension(product_key),
    FOREIGN KEY (time_key) REFERENCES Time_Dimension(time_key),
    FOREIGN KEY (date_key) REFERENCES Date_Dimension(date_key),
    FOREIGN KEY (warehouse_key) REFERENCES Warehouse_Dimension(warehouse_key),
    FOREIGN KEY (promotion_key) REFERENCES Promotion_Dimension(promotion_key),
    FOREIGN KEY (customer_key) REFERENCES Customer_Dimension(customer_key),
    FOREIGN KEY (location_key) REFERENCES Location_Dimension(location_key),
    FOREIGN KEY (website_key) REFERENCES Website_Dimension(website_key),
    FOREIGN KEY (navigation_key) REFERENCES Navigation_Dimension(navigation_key),
    FOREIGN KEY (ship_mode_key) REFERENCES Ship_Mode_Dimension(ship_mode_key)
);