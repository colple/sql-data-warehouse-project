/*
===============================================================================
Gold Layer: Dimension Product
===============================================================================
Script Purpose:
    This script creates the 'gold.dim_products' view by joining 
    Silver layer tables. It follows a structured approach:
    1. Initial Join Exploration
    2. Data Integrity & Duplicate Checks
    3. Business Logic Implementation (Filtering Active Products)
    4. Final View Creation with Surrogate Keys

Note:
- This is a DIMENSION table.
- A Surrogate Key (product_key) is generated to uniquely identify each record.
- Historical data is filtered to keep only active products (SCD Type 0/1 approach).
===============================================================================
*/

-- =============================================================================
-- 1. ANALYZING: Explore & understand the business objects
-- =============================================================================
-- The CRM table contains historical product data. 
-- Business Rule: We only keep currently active products (where prd_end_dt is NULL).

SELECT
    pn.prd_id,
    pn.prd_key,
    pn.cat_id,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
FROM silver.crm_prd_info AS pn
WHERE pn.prd_end_dt IS NULL; -- Filter out historical data

-- Goal: Join CRM Product info with ERP Category metadata.
-- Strategy: Use LEFT JOIN with CRM as the master table to ensure core product catalog integrity.
SELECT
    pn.prd_id,
    pn.prd_key,
    pn.cat_id,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc 
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

-- =============================================================================
-- 2. VALIDATING: Data Integration Checks
-- =============================================================================

-- Check 1: Verify if any duplicates were introduced by the joining logic
-- Expected result: 0 rows (Unique prd_key for active products)
WITH joined_table AS (
    SELECT pn.prd_key
    FROM silver.crm_prd_info AS pn
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc 
    ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NULL
)
SELECT 
    prd_key,
    COUNT(*) AS duplicates
FROM joined_table
GROUP BY prd_key
HAVING COUNT(*) > 1
ORDER BY duplicates DESC;

-- Check 2: Inspect redundant info after join
-- Observation: No redundant or overlapping columns between CRM and ERP for this dimension.

-- =============================================================================
-- 3. CODING: Building the Gold View
-- =============================================================================
-- Steps: Apply friendly names, logical grouping, and Surrogate Key generation.

DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT
    -- SURROGATE KEY: System-generated unique identifier for the star schema.
    -- Essential for performance and historical tracking.
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,    
    -- BUSINESS KEYS & ATTRIBUTES (Renamed to friendly names)
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,   
    -- DATES
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc 
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Ensures only current products are in the dimension

-- =============================================================================
-- 4. FINAL QUALITY CHECK
-- =============================================================================
-- Final view validation
SELECT * FROM gold.dim_products;