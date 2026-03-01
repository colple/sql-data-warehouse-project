/*
===============================================================================
Gold Layer: Fact Sales
===============================================================================
Script Purpose:
    This script creates the 'gold.fact_sales' view by joining the Silver 
    sales data with Gold dimensions. It follows a structured approach:
    1. Initial Exploration of Sales Data
    2. Data Lookup: Replacing Business Keys with Surrogate Keys
    3. Final View Creation with Optimized Column Grouping

Note:
- This is a FACT table, containing measures and keys to dimensions.
- Surrogate Keys (product_key, customer_key) are used for star schema integrity.
===============================================================================
*/

-- =============================================================================
-- 1. ANALYZING: Explore & understand the business objects
-- =============================================================================
-- Reviewing the raw sales details from the Silver layer.
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cus_id,
    sls_ord_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price,
    dwh_insertion_date
FROM silver.crm_sales_details;

-- =============================================================================
-- 2. CODING: Building the Gold View
-- =============================================================================
-- Steps: 
-- 1. Data Lookup: Use dimension surrogate keys instead of business IDs.
-- 2. Rename & Reorder: Apply business-friendly names and logical grouping.

-- Step 1 & 2: Testing the Join Logic and Column Selection
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,   -- Lookup from gold.dim_products
    cu.customer_key AS customer_key, -- Lookup from gold.dim_customers
    sd.sls_ord_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customers AS cu ON sd.sls_cus_id = cu.customer_id
LEFT JOIN gold.dim_products AS pr ON sd.sls_prd_key = pr.product_number;

-- Step 3: Create the Final View
DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT
    -- DIMENSION KEYS (Surrogate Keys for Star Schema)
    sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,    
    -- DATES
    sd.sls_ord_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,    
    -- MEASURES (Quantitative values)
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customers AS cu ON sd.sls_cus_id = cu.customer_id
LEFT JOIN gold.dim_products AS pr ON sd.sls_prd_key = pr.product_number;

-- =============================================================================
-- 3. FINAL QUALITY CHECK (QA)
-- =============================================================================
-- 1. Final view validation
SELECT * FROM gold.fact_sales LIMIT 100;

-- 2. Foreign Key Integrity Check
-- Goal: Check if all dimension tables can successfully join to fact table.
-- Expected Result: Empty result (No rows where keys are NULL).
SELECT *
FROM gold.fact_sales AS sa
LEFT JOIN gold.dim_products AS pr ON sa.product_key = pr.product_key
LEFT JOIN gold.dim_customers AS cu ON sa.customer_key = cu.customer_key
WHERE pr.product_key IS NULL OR cu.customer_key IS NULL;