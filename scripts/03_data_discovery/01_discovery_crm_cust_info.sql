/*
===============================================================================
Data Quality Discovery: bronze.crm_cust_info
===============================================================================
Script Purpose:
    - Audit the Customer source table to identify quality issues and duplicates.
    - Validate VARCHAR to Typed conversions (INT, DATE).
    - Define Business Rules for deduplication and data standardization.

Discovery Areas for this Table:
    1. Identity Integrity: Handling NULLs and duplicates in 'cst_id' (Primary Key).
    2. Data Standardization: Identifying whitespace issues across all string fields.
    3. Categorical Normalization: Mapping coded values (Gender, Marital Status) 
       to user-friendly descriptions.
    4. Deduplication Strategy: Using 'cst_create_date' to isolate the latest 
       customer profile.

Cleaning Strategy:
    - Systematic TRIM() applied to all VARCHAR columns.
    - Filter out records with NULL 'cst_id' to maintain referential integrity.
    - Standardize 'n/a' for missing categorical values to ensure reporting consistency.
===============================================================================
*/

-- Initial Inspection
SELECT * FROM bronze.crm_cust_info LIMIT 100;

-- =============================================================================
-- 1. Column: cst_id (Primary Key)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Current data is stored as VARCHAR in Bronze. 
-- Must be CAST to INTEGER during the Silver layer load.
-- Verification: Check for any non-numeric values that would break the conversion.
SELECT * FROM bronze.crm_cust_info 
WHERE cst_id ~ '[^0-9]';

-- B. Check for Unwanted Spaces
-- Expectation: No result.
-- Finding: Identified some records with leading/trailing spaces.
SELECT cst_id
FROM bronze.crm_cust_info
WHERE LENGTH(CAST(cst_id AS TEXT)) != LENGTH(TRIM(CAST(cst_id AS TEXT)));

-- C. Check for Nulls or Duplicates
-- Expectation: cst_id should be unique and non-null.
-- Finding: Duplicates and NULL values identified. 
-- Data Enrichment Strategy: Handle duplicates by selecting the latest record based on 'cst_create_date'.
SELECT 
    cst_id,
    COUNT(*) AS duplicates
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
ORDER BY duplicates DESC;

-- D. Detailed Duplicate Analysis
-- Objective: Understand why IDs are duplicated.
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id IN (
    SELECT cst_id
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1
) OR cst_id IS NULL
ORDER BY cst_id ASC;

/* BUSINESS RULE FOR SILVER:
- Apply TRIM() to ensure keys are clean.
- Filter out NULLs as they provide no way to identify the customer.
- Use ROW_NUMBER() PARTITION BY cst_id ORDER BY cst_create_date DESC to keep only the latest update.
*/

-- =============================================================================
-- 2. Column: cst_key ((Business Key))
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50). 
-- Must be cleaned using TRIM() and handled for potential NULLs/Empty values.

-- B. Check for Unwanted Spaces
-- Expectation: No result.
-- Finding: KO (Spaces detected).
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- C. Check for Nulls or Duplicates in Business Key
-- NOTE: Built progressively.
-- Filter applied: Keeping only unique records (flag_duplicate = 1) and non-null cst_id.
WITH ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_duplicate
    FROM bronze.crm_cust_info
),
unique_customers AS (
    SELECT *
    FROM ranked_customers
    WHERE flag_duplicate = 1 AND cst_id IS NOT NULL
)
SELECT 
    cst_key,
    COUNT(*) AS duplicates
FROM unique_customers
GROUP BY cst_key
HAVING COUNT(*) > 1 OR cst_key IS NULL OR cst_key = ''
ORDER BY duplicates DESC;

-- D. Final Deduplication Strategy for Business Key
-- Objective: Identify if duplicates still exist at 'cst_key' level after 'cst_id' cleaning.
SELECT *
FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_key ORDER BY cst_create_date DESC) AS flag_duplicate
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) ranked_customers
WHERE flag_duplicate > 1;

/* BUSINESS RULE FOR SILVER:
- Apply TRIM() to cst_key.
- Deduplication: In the final load, we will partition by cst_key to ensure 
  the Business Key uniqueness, keeping the latest record (cst_create_date DESC).
- Missing values: Ensure NULLs or empty strings are handled (target: 'n/a').
*/

-- =============================================================================
-- 3. Column: cst_firstname
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50).

-- B. Check for Unwanted Spaces
-- Expectation: No result.
-- Finding: Identified records with leading/trailing spaces.
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- =============================================================================
-- 4. Column: cst_lastname
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50).

-- B. Check for Unwanted Spaces
-- Expectation: No result.
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- =============================================================================
-- 5. Column: cst_marital_status
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50).

-- B. Check for Unwanted Spaces
-- Expectation: No result.
SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE LENGTH(cst_marital_status) != LENGTH(TRIM(cst_marital_status));

-- C. Consistency Check (Low Cardinality)
-- Objective: Visualize the distinct coded values before normalization.
SELECT DISTINCT(TRIM(cst_marital_status))
FROM bronze.crm_cust_info;

-- D. Data Normalization / Standardization
-- Business Rule: Maps coded values to meaningful, user-friendly descriptions.
-- Also handling missing values by filling them with a default value ('n/a').
SELECT 
    cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a'
    END AS standardized_marital_status
FROM bronze.crm_cust_info;

-- =============================================================================
-- 6. Column: cst_gender
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50).

-- B. Check for Unwanted Spaces
SELECT cst_gender
FROM bronze.crm_cust_info
WHERE LENGTH(cst_gender) != LENGTH(TRIM(cst_gender));

-- C. Consistency Check (Low Cardinality)
SELECT DISTINCT(TRIM(cst_gender))
FROM bronze.crm_cust_info;

-- D. Data Normalization / Standardization
-- Business Rule: Maps codes 'M' and 'F' to 'Male' and 'Female'.
-- Also handling missing values by filling them with a default value ('n/a').
SELECT 
    cst_gender,
    CASE WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
         WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
         ELSE 'n/a'
    END AS standardized_gender
FROM bronze.crm_cust_info;

-- =============================================================================
-- 7. Column: cst_create_date
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR in the bronze layer. 
-- Must be CAST to DATE in the Silver layer.

-- B. Integrity Check (Future Dates)
-- Expectation: No future dates should exist.
-- Finding: OK (No future dates detected).
SELECT DISTINCT cst_create_date
FROM bronze.crm_cust_info
WHERE CAST(cst_create_date AS DATE) > CURRENT_DATE;

-- =============================================================================
-- FINAL CLEANING & TRANSFORMATION (The "Golden" Logic)
-- =============================================================================
-- Purpose: This query consolidates all cleaning rules identified during discovery.
-- 1. Filters out Null Primary Keys.
-- 2. Applies TRIM() to all VARCHAR columns.
-- 3. Performs Data Normalization (Gender/Marital Status).
-- 4. Handles Duplicates by keeping the latest record (cst_create_date).
-- =============================================================================

SELECT * FROM (
    SELECT 
        CAST(TRIM(cst_id) AS INTEGER) AS cst_id,
        TRIM(cst_key) AS cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
             WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
             ELSE 'n/a'
        END AS cst_marital_status, -- Data Normalization
        CASE WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
             WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
             ELSE 'n/a'
        END AS cst_gender, -- Data Normalization
        CAST(cst_create_date AS DATE) AS cst_create_date,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_duplicate -- According to the user's selection
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL -- Eliminating Null Primary Keys
) t 
WHERE flag_duplicate = 1; -- Keeping only the most recent unique records

-- =============================================================================
-- FINAL DATA QUALITY SUMMARY: bronze.crm_cust_info
-- =============================================================================

/* DEDUPLICATION & NULL HANDLING:
- Identification: Eliminate NULLs and empty values for the Primary Key (cst_id), 
  as no information is available to identify the customer.
- Selection Logic: Regarding duplicates, we will retain the record with the most 
  complete or latest information (selon la sélection de l'utilisateur).
  
CLEANING BEST PRACTICES:
- Whitespace Management: Since several VARCHAR columns contain unwanted spaces, 
  a systematic TRIM() will be applied to all string columns in this table.
  
TRANSFORMATION STRATEGY:
- Use ROW_NUMBER() PARTITION BY cst_id ORDER BY cst_create_date DESC to isolate the 
  most recent record per customer.
*/


TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gender,
	cst_create_date
)
SELECT
	CAST(cst_id AS INTEGER) AS cst_id,
	TRIM(cst_key)  AS cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a' END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		 WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		 ELSE 'n/a' END AS cst_gender, 
	CAST(cst_create_date AS DATE) AS cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_key ORDER BY cst_create_date DESC) AS flag_duplicate
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) AS ranked_customers
WHERE flag_duplicate = 1;