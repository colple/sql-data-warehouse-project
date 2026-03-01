-- 1. Analyzing: Explore & understand the business objects
-- 2. Coding: Data Integration
-- * Build the business object
-- * Choose type: Dimension vs Fact
-- * Rename to friendly name
-- 3. Validating: Data Integration Checks
-- 4. Doc & version

-- Data Modeling
-- Regarder le modèle d'intégration pour construire les tables de fact et dimension (ici modèle en étoile)
-- On voit que l'on peut créer 3 tables
-- * Table des faits: sales
-- * dim_customers: les 3 tables cust
-- * dim_products: les 2 tables produits

-- Pas de store procedure

/*
===============================================================================
Gold Layer: Dimension Customer
===============================================================================
Script Purpose:
    This script creates the 'gold.dim_customers' view by joining 
    Silver layer tables. It follows a structured approach:
    1. Initial Join Exploration
    2. Data Integrity & Duplicate Checks
    3. Business Logic Implementation (Gender Master Source)
    4. Final View Creation with Surrogate Keys

Note:
- This is a DIMENSION table.
- A Surrogate Key (customer_key) is generated to uniquely identify each record.
- Only the 'Gender' column required complex transformation (CRM as master).
===============================================================================
*/

-- =============================================================================
-- 1. ANALYZING: Explore & understand the business objects
-- =============================================================================
-- Goal: Join the 3 customer-related tables (CRM + ERP)
-- Strategy: Use LEFT JOIN with the primary CRM table to avoid data loss.

SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gender,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la ON ci.cst_key = la.cid;

-- =============================================================================
-- 2. VALIDATING: Data Integration Checks
-- =============================================================================

-- Check 1: Verify if any duplicates were introduced by the joining logic
-- Expected result: 0 rows (Unique cst_id)
WITH joined_table AS (
    SELECT ci.cst_id
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la  ON ci.cst_key = la.cid
)
SELECT 
    cst_id,
    COUNT(*) AS duplicates
FROM joined_table
GROUP BY cst_id
HAVING COUNT(*) > 1
ORDER BY duplicates DESC;

-- Check 2: Inspect redundant info for 'gender' (CRM vs ERP)
-- Business Rule: CRM is the master source. Fallback to ERP if CRM is 'n/a'.
SELECT DISTINCT
    ci.cst_gender,
    ca.gen,
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender 
        ELSE COALESCE(ca.gen, 'n/a') 
    END AS cleaned_gender
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la  ON ci.cst_key = la.cid
ORDER BY ci.cst_gender, ca.gen;

-- =============================================================================
-- 3. CODING: Building the Gold View
-- =============================================================================
-- Steps: Friendly names, Logical grouping, and Surrogate Key generation.

DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT
    -- SURROGATE KEY: System-generated unique identifier for the star schema.
    -- Essential for performance and historical tracking (SCD).
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,    
    -- BUSINESS KEYS & ATTRIBUTES (Renamed to friendly names)
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,   
    -- GENDER LOGIC (Consolidated)
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender 
        ELSE COALESCE(ca.gen, 'n/a') 
    END AS gender,   
    -- DATES
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date  
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la  ON ci.cst_key = la.cid;

-- =============================================================================
-- 4. FINAL QUALITY CHECK
-- =============================================================================
SELECT * FROM gold.dim_customers;

-- Final distribution check
SELECT gender, COUNT(*) FROM gold.dim_customers GROUP BY gender;
