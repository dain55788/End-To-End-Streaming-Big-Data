-- Drops the schema for all tables in the database.

-- Disable foreign key checks to allow dropping tables
SET FOREIGN_KEY_CHECKS = 0;

-- Drop fact table
DROP TABLE IF EXISTS fact_sales;

-- Drop dimension tables
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_payment;

-- Enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Tables dropped successfully.