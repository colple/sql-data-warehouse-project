/*
===============================================================================
Data Quality Discovery: bronze.erp_loc_a101
===============================================================================
Script Purpose:
    - Audit the ERP Location source (System A101).
    - Validate alignment with CRM customer keys.
    - Standardize Country names for reporting.

Cleaning Strategy:
    - Key Cleaning: Remove hyphens '-' and apply TRIM to enable joins with 'cst_key'.
    - Country Normalization: Standardize 'US', 'USA' and 'DE' into full country names.
===============================================================================
*/

-- Initial Inspection
SELECT * FROM bronze.erp_loc_a101 LIMIT 100;

-- =============================================================================
-- 1. Column: cid (Customer ID)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50). 
-- Needs to be cleaned to match CRM 'cst_key'.

-- B. Unwanted Spaces Check
-- Expectation: No result.
SELECT cid 
FROM bronze.erp_loc_a101 
WHERE LENGTH(cid) != LENGTH(TRIM(cid));

-- C. Primary Key Integrity (Nulls & Duplicates)
-- Expectation: No result.
SELECT 
    cid,
    COUNT(*) AS duplicates
FROM bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL OR cid = '';

-- D. Referential Integrity Check (CRM Sync)
-- Finding: KO. Presence of unwanted hyphens '-' prevents direct matching.
SELECT DISTINCT cid 
FROM bronze.erp_loc_a101
WHERE TRIM(cid) NOT IN (
    SELECT DISTINCT TRIM(cst_key) 
    FROM bronze.crm_cust_info
);

-- E. CORRECTIVE PATCH: Remove hyphens and apply TRIM
-- Objective: Test the logic to normalize the ID.
SELECT
    cid AS original_cid,
    CASE 
        WHEN TRIM(cid) LIKE '%-%' THEN REPLACE(TRIM(cid), '-', '') 
        ELSE TRIM(cid) 
    END AS cleaned_cid
FROM bronze.erp_loc_a101;

-- F. PATCH VALIDATION: Re-check sync after cleaning
-- Expectation: No Result (All cleaned IDs should now match CRM keys).
-- Finding: OK (selon la sélection de l'utilisateur).
WITH cleaned_data AS (
    SELECT
        CASE 
            WHEN TRIM(cid) LIKE '%-%' THEN REPLACE(TRIM(cid), '-', '') 
            ELSE TRIM(cid) 
        END AS new_cid
    FROM bronze.erp_loc_a101
)
SELECT DISTINCT new_cid
FROM cleaned_data
WHERE new_cid NOT IN (
    SELECT DISTINCT TRIM(cst_key)
    FROM bronze.crm_cust_info
);

/* BUSINESS RULE FOR SILVER:
- Clean 'cid' by removing hyphens and trimming spaces to ensure referential integrity.
*/

-- =============================================================================
-- 2. Column: cntry (Country)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50).

-- B. Unwanted Spaces Check
-- Expectation: No result.
SELECT cntry
FROM bronze.erp_loc_a101
WHERE LENGTH(cntry) != LENGTH(TRIM(cntry));

-- C. Consistency Check (Distinct values)
-- Finding: KO. Mix of 'US', 'USA', 'DE' and potential NULLs.
SELECT DISTINCT TRIM(cntry)
FROM bronze.erp_loc_a101;

-- D. CORRECTIVE PATCH: Standardize Country Names
-- Objective: Map codes to full names and handle missing values.
SELECT
    cntry AS original_cntry,
    CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
         WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
         ELSE TRIM(cntry) 
    END AS standardized_cntry
FROM bronze.erp_loc_a101;

-- E. PATCH VALIDATION: Verify final mapping
-- Expectation: Only standardized names or 'n/a' should remain.
WITH cleaned_data AS (
    SELECT
        CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
             WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
             WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
             ELSE TRIM(cntry) 
        END AS cntry
    FROM bronze.erp_loc_a101
)
SELECT DISTINCT cntry
FROM cleaned_data;

-- =============================================================================
-- FINAL DATA QUALITY SUMMARY: bronze.erp_loc_a101
-- =============================================================================
/*
IDENTIFICATION & INTEGRITY:
- Key Alignment: The 'cid' column requires removing hyphens to match crm_cust_info.

TRANSFORMATION STRATEGY:
- Geographic Normalization: Country codes standardized (selon la sélection de l'utilisateur).
*/

-- =============================================================================
-- IMPLEMENTATION (For silver.load_silver_layer)
-- =============================================================================

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    CASE WHEN TRIM(cid) LIKE '%-%' THEN REPLACE(TRIM(cid), '-', '') ELSE TRIM(cid) END AS cid,
    CASE 
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States' 
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany' 
        WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a' 
        ELSE cntry 
    END AS cntry
FROM (
    SELECT *, 
           COUNT(*) OVER (PARTITION BY cid) AS occurrence_count 
    FROM bronze.erp_loc_a101 WHERE cid IS NOT NULL
) AS t
WHERE occurrence_count = 1;