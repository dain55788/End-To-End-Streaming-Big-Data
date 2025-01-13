-- Deletes all records in each table but retains the schema.

-- Disable foreign key checks to allow deletion without constraint errors
SET FOREIGN_KEY_CHECKS = 0;

-- Fact table
DELETE FROM sales_fact_table;

-- Dimension tables
DELETE FROM customer_dimension;
DELETE FROM date_dimension;
DELETE FROM time_dimension;
DELETE FROM location_dimension;
DELETE FROM website_dimension;
DELETE FROM navigation_dimension;
DELETE FROM ship_mode_dimension;
DELETE FROM warehouse_dimension;
DELETE FROM promotion_dimension;
DELETE FROM product_dimension;

-- Enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Optional: Reset AUTO_INCREMENT values for all tables
ALTER TABLE sales_fact_table AUTO_INCREMENT = 1;
ALTER TABLE customer_dimension AUTO_INCREMENT = 1;
ALTER TABLE product_dimension AUTO_INCREMENT = 1;
ALTER TABLE promotion_dimension AUTO_INCREMENT = 1;
ALTER TABLE location_dimension AUTO_INCREMENT = 1;
ALTER TABLE website_dimension AUTO_INCREMENT = 1;
ALTER TABLE navigation_dimension AUTO_INCREMENT = 1;
ALTER TABLE ship_mode_dimension AUTO_INCREMENT = 1;
ALTER TABLE warehouse_dimension AUTO_INCREMENT = 1;
ALTER TABLE date_dimension AUTO_INCREMENT = 1;
ALTER TABLE time_dimension AUTO_INCREMENT = 1;

-- Deletion complete.
