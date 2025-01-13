-- Drops the schema for all tables in the database.

-- Disable foreign key checks to allow dropping tables
SET FOREIGN_KEY_CHECKS = 0;

-- Drop fact table
DROP TABLE IF EXISTS sales_fact_table;

-- Drop dimension tables
DROP TABLE IF EXISTS customer_dimension;
DROP TABLE IF EXISTS date_dimension;
DROP TABLE IF EXISTS location_dimension;
DROP TABLE IF EXISTS navigation_dimension;
DROP TABLE IF EXISTS product_dimension;
DROP TABLE IF EXISTS promotion_dimension;
DROP TABLE IF EXISTS ship_mode_dimension;
DROP TABLE IF EXISTS time_dimension;
DROP TABLE IF EXISTS warehouse_dimension;
DROP TABLE IF EXISTS website dimension;

-- Enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Tables dropped successfully.