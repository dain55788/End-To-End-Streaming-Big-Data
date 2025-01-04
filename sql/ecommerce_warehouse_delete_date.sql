-- Deletes all records in each table but retains the schema.

-- Disable foreign key checks to allow deletion without constraint errors
SET FOREIGN_KEY_CHECKS = 0;

-- Fact table
DELETE FROM fact_sales;

-- Dimension tables
DELETE FROM dim_customer;
DELETE FROM dim_product;
DELETE FROM dim_date;
DELETE FROM dim_location;
DELETE FROM dim_payment;

-- Enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Optional: Reset AUTO_INCREMENT values for all tables
ALTER TABLE fact_sales AUTO_INCREMENT = 1;
ALTER TABLE dim_customer AUTO_INCREMENT = 1;
ALTER TABLE dim_product AUTO_INCREMENT = 1;
ALTER TABLE dim_date AUTO_INCREMENT = 1;
ALTER TABLE dim_location AUTO_INCREMENT = 1;
ALTER TABLE dim_payment AUTO_INCREMENT = 1;

-- Deletion complete.
