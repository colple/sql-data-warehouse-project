/*
===============================================================================
Data Quality Discovery: bronze.erp_px_cat_g1v2
===============================================================================
Script Purpose:
    - Audit the ERP Product Category source (System G1V2).
    - Validate alignment with CRM product category keys.
    - Ensure unique identifiers for the product hierarchy.

Cleaning Strategy:
    - Category Mapping: Ensure 'id' matches the derived keys from CRM products.
    - General Cleaning: Apply TRIM to all descriptive fields for reporting consistency.
===============================================================================
*/

-- Initial Inspection
SELECT * FROM bronze.erp_px_cat_g1v2 LIMIT 100;

-- =============================================================================
-- 1. Column: id (Category ID)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50).
-- Matches 'cat_key' (e.g., 'AC_HE') derived from CRM.

-- B. Unwanted Spaces Check
-- Expectation: No result.
SELECT id 
FROM bronze.erp_px_cat_g1v2 
WHERE LENGTH(id) != LENGTH(TRIM(id));

-- C. Primary Key Integrity (Nulls & Duplicates)
-- Expectation: No result.
SELECT 
    id, 
    COUNT(*) AS duplicates
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL OR id = '';

-- D. Referential Integrity Check (CRM Mismatch)
-- Objective: Ensure all products in CRM have a corresponding category entry.
-- Finding: OK (selon la sélection de l'utilisateur).
WITH cleaned_crm_prd_info AS (
    SELECT
	    CAST(TRIM(prd_id) AS INTEGER) AS prd_id,
	    SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
	    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_key,
	    TRIM(prd_nm) AS prd_nm,
	    CAST(prd_cost AS DECIMAL(18,2)) AS prd_cost,
	    COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0) AS prd_cost,
	    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
		     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		     ELSE 'n/a' END AS prd_line,
	    CAST(prd_start_dt AS DATE) AS prd_start_dt,
	    LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY CAST(prd_start_dt AS DATE)) - 1 AS prd_end_dt
    FROM bronze.crm_prd_info
)
SELECT DISTINCT(id)
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
	SELECT DISTINCT cat_key
	FROM cleaned_crm_prd_info
)

-- =============================================================================
-- 2. Columns: cat, subcat, maintenance
-- =============================================================================

-- A. Data Type Assessment: All VARCHAR(50).

-- B. Unwanted Spaces Check
-- Finding: OK.
SELECT cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2
WHERE LENGTH(cat) != LENGTH(TRIM(cat))
   OR LENGTH(subcat) != LENGTH(TRIM(subcat))
   OR LENGTH(maintenance) != LENGTH(TRIM(maintenance));

-- C. Distinct Values & Consistency Check
-- Finding: OK. Standardized hierarchy and sub-categories.
SELECT DISTINCT cat, subcat, maintenance 
FROM bronze.erp_px_cat_g1v2
ORDER BY cat, subcat;

-- =============================================================================
-- FINAL DATA QUALITY SUMMARY: bronze.erp_px_cat_g1v2
-- =============================================================================
/*
IDENTIFICATION & INTEGRITY:
- Category Mapping: Confirmed unique and perfectly aligned with keys in CRM.

TRANSFORMATION STRATEGY:
- General Cleaning: Systematic TRIM() on all fields to prevent future join or 
  reporting issues (selon la sélection de l'utilisateur).
*/

-- =============================================================================
-- IMPLEMENTATION (For silver.load_silver_layer)
-- =============================================================================

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    TRIM(id) AS id,
    TRIM(cat) AS cat,
    TRIM(subcat) AS subcat,
    TRIM(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2;


