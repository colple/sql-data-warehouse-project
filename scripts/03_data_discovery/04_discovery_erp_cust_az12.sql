/*
===============================================================================
Data Quality Discovery: bronze.erp_cust_az12
===============================================================================
Script Purpose:
    - Audit the ERP Customer source (System AZ12).
    - Validate alignment with CRM customer keys.
    - Standardize Birth Dates and Gender codes.

Cleaning Strategy:
    - Key Cleaning: Remove 'NAS' prefix from 'cid' to enable joins with 'cst_key'.
    - Gender Normalization: Consolidate (M, Male, F, Female) into (Male, Female).
    - Date Filtering: Nullify unrealistic birth dates (future or > 120 years).
===============================================================================
*/

-- Initial Inspection
SELECT * FROM bronze.erp_cust_az12 LIMIT 100;

-- =============================================================================
-- 1. Column: cid (Customer ID)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR. 
-- Must match 'cst_key' format for future joins.

-- B. Unwanted Spaces Check
-- Expectation: No result.
SELECT cid 
FROM bronze.erp_cust_az12 
WHERE LENGTH(cid) != LENGTH(TRIM(cid));

-- B. Primary Key Integrity (Nulls & Duplicates)
-- Expectation: No result.
SELECT 
    cid, 
    COUNT(*) AS duplicates 
FROM bronze.erp_cust_az12 
GROUP BY cid HAVING 
COUNT(*) > 1 OR cid IS NULL OR CID = '';

-- C. Referential Integrity Check (CRM Sync)
-- Expectation: No result.
-- Finding: KO. Presence of 'NAS' prefix prevents direct matching.
SELECT DISTINCT cid 
FROM bronze.erp_cust_az12
WHERE TRIM(cid) NOT IN (
    SELECT DISTINCT TRIM(cst_key) 
    FROM bronze.crm_cust_info
    );

-- D. CORRECTIVE PATCH: Remove 'NAS' characters
-- Objective: Test the logic to strip the prefix.
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;

-- E. PATCH VALIDATION: Re-check sync after cleaning
-- Expectation: No Result (All IDs should now match CRM keys).
-- Finding: OK (selon la sélection de l'utilisateur).
WITH cleaned_data AS (
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END AS cid,
        bdate,
        gen
    FROM bronze.erp_cust_az12 
)
SELECT DISTINCT cid
FROM cleaned_data
WHERE TRIM(cid) NOT IN (
    SELECT DISTINCT TRIM(cst_key)
    FROM bronze.crm_cust_info
);

/* BUSINESS RULE FOR SILVER:
- Clean 'cid' by removing the 'NAS' prefix if present to ensure referential integrity.
*/

-- =============================================================================
-- 2. Column: bdate (Birth Date)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Current data is stored as VARCHAR in Bronze. 
-- Must be CAST to DATE during the Silver layer load.

-- B. Validity Check (Future or Out-of-Range)
-- Finding: KO (16 records with future dates identified).
SELECT bdate FROM bronze.erp_cust_az12
WHERE CAST(bdate AS DATE) > CURRENT_DATE 
   OR CAST(bdate AS DATE) < CURRENT_DATE - INTERVAL '120 years'
ORDER BY CAST(bdate AS DATE) DESC;

-- C. CORRECTIVE PATCH: Put unrealistic dates to NULL
-- Objective: Map outliers to NULL to maintain demographic integrity.
SELECT
    CASE WHEN CAST(bdate AS DATE) > CURRENT_DATE 
           OR CAST(bdate AS DATE) < CURRENT_DATE - INTERVAL '120 years'
         THEN NULL
         ELSE CAST(bdate AS DATE) 
    END AS bdate
FROM bronze.erp_cust_az12;

/* BUSINESS RULE FOR SILVER:
- Set out-of-range birth dates to NULL.
*/

-- =============================================================================
-- 3. Column: gen (Gender)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50). 
-- Must be standardized to 'Male' or 'Female' to match the CRM dimension.

-- B. Check for Unwanted Spaces
-- Expectation: No result.
SELECT gen
FROM bronze.erp_cust_az12
WHERE LENGTH(gen) != LENGTH(TRIM(gen));

-- C. Consistency Check (Low Cardinality)
-- Objective: Visualize the distinct coded values before normalization.
-- Finding: KO (Mix of 'M', 'F', 'Male', 'Female', etc.).
SELECT DISTINCT TRIM(gen)
FROM bronze.erp_cust_az12;

-- D. CORRECTIVE PATCH: Data Normalization / Standardization
-- Business Rule: Maps diverse source codes to unified 'Male' and 'Female' descriptions.
-- Missing or unexpected values are mapped to 'n/a'.
SELECT
    gen AS original_gen,
    CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         ELSE 'n/a' 
    END AS standardized_gen
FROM bronze.erp_cust_az12;

-- E. PATCH VALIDATION: Verify if the mapping covers all cases
-- Expectation: Only 'Male', 'Female', or 'n/a' should remain.
-- Finding: OK (selon la sélection de l'utilisateur).
WITH cleaned_data AS (
    SELECT
        CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
             WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
             ELSE 'n/a' 
        END AS gen
    FROM bronze.erp_cust_az12
)
SELECT DISTINCT gen 
FROM cleaned_data;

/* BUSINESS RULE FOR SILVER:
- Apply TRIM() and UPPER() for consistent mapping.
- Standardize labels to 'Male', 'Female', or 'n/a' for reporting consistency.
*/

-- =============================================================================
-- 4. FINAL INTEGRATION TEST (Consistency with CRM)
-- =============================================================================
-- Objective: Ensure the final transformation logic results in clean, joinable data.
-- Finding: OK. Verified alignment with crm_cust_info (selon la sélection de l'utilisateur).

WITH cleaned_data AS (
    SELECT
        TRIM(CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END) AS cid,
        CASE 
            WHEN CAST(bdate AS DATE) > CURRENT_DATE 
              OR CAST(bdate AS DATE) < CURRENT_DATE - INTERVAL '120 years'
            THEN NULL
            ELSE CAST(bdate AS DATE) 
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'n/a' 
        END AS gen
    FROM bronze.erp_cust_az12 
) 
SELECT *
FROM cleaned_data
WHERE cid IN (
    SELECT DISTINCT cst_key
    FROM bronze.crm_cust_info
);

-- Final Select for Procedure implementation
SELECT
    TRIM(CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END) AS cid,
    CASE WHEN CAST(bdate AS DATE) > CURRENT_DATE OR CAST(bdate AS DATE) < CURRENT_DATE - INTERVAL '120 years'
            THEN NULL
        ELSE CAST(bdate AS DATE) END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         ELSE 'n/a' END AS gen
FROM bronze.erp_cust_az12;

-- =============================================================================
-- FINAL DATA QUALITY SUMMARY: bronze.erp_cust_az12
-- =============================================================================
/*
IDENTIFICATION & INTEGRITY:
- Key Alignment: The 'cid' column requires removing the 'NAS' prefix to match 
  the CRM business keys (cst_key).

TRANSFORMATION STRATEGY:
- Date Validation: Birth dates are capped at 120 years; future dates are nullified.
- Gender Standardization: Mapping diverse source codes to a unified 'Male/Female' format.
*/

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    TRIM(CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END) AS cid,
    CASE WHEN CAST(bdate AS DATE) > CURRENT_DATE OR CAST(bdate AS DATE) < CURRENT_DATE - INTERVAL '120 years'
            THEN NULL
        ELSE CAST(bdate AS DATE) END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         ELSE 'n/a' END AS gen
FROM bronze.erp_cust_az12;