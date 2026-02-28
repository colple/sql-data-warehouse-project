/*
===============================================================================
Data Quality Discovery: bronze.crm_prd_info
===============================================================================
Script Purpose:
    - Audit the Product dimension to identify technical and logical anomalies.
    - Validate VARCHAR to Typed conversions (INT, DECIMAL, DATE).
    - Establish Business Rules for SCD (Slowly Changing Dimensions) logic.

Discovery Areas for this Table:
    1. Key Derivation: Extracting 'cat_key' and cleaning 'prd_key' for ERP joins.
    2. Data Range Validation: Identifying negative costs or invalid pricing.
    3. Normalization: Standardizing 'prd_line' codes into friendly descriptions.
    4. Timeline Integrity: Using LEAD() to fix overlapping start/end dates.

Cleaning Strategy:
    - Systematic TRIM() on all string fields.
    - COALESCE(cost, 0) for missing price information.
    - Logic enrichment to ensure continuous product history without date gaps.
===============================================================================
*/

-- Initial Inspection
SELECT * FROM bronze.crm_prd_info LIMIT 100;

-- =============================================================================
-- 1. Column: prd_id (Primary Key)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR. Must be CAST to INTEGER in Silver.
SELECT * FROM bronze.crm_prd_info 
WHERE prd_id ~ '[^0-9]';

-- B. Check for Unwanted Spaces
-- Expectation: No result.
SELECT prd_id
FROM bronze.crm_prd_info
WHERE LENGTH(prd_id) != LENGTH(TRIM(prd_id));

-- C. Check for Nulls or Duplicates
-- Expectation: No Result.
SELECT 
    prd_id,
    COUNT(*) AS duplicates
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- =============================================================================
-- 2. Column: prd_key (Business Key & Joins)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50). Used for joins with ERP and Sales.

-- B. Check for Unwanted Spaces
-- Expectation: No result.
SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key);

-- C. Key Derivation & Formatting
-- Business Rule: Extract cat_key (first 5 chars, '-' to '_') and clean prd_key.
SELECT 
    prd_key, 
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_key,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS clean_prd_key
FROM bronze.crm_prd_info;

-- D. Referential Integrity Check (ERP Join)
-- Finding: 1 cat_key (CO_PE) is missing in erp_px_cat_g1v2.
SELECT DISTINCT REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS missing_cat_key
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
    SELECT DISTINCT cat_key 
	FROM bronze.erp_px_cat_g1v2
);

-- E. Referential Integrity Check (Sales Join)
-- Expectation: All sold products should exist in the product master.
-- Finding: Some products in crm_prd_info are NOT in sales_details.
-- NOTE: This is acceptable as these represent "unsold products" or new inventory.
SELECT *
FROM bronze.crm_prd_info
WHERE SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) NOT IN (
    SELECT DISTINCT sls_prd_key
    FROM bronze.crm_sales_details
);

-- F. Duplicate Analysis (Historization/SCD)
-- Finding: Duplicates exist (same key, different dates).
SELECT prd_key, COUNT(*) AS duplicates
FROM bronze.crm_prd_info
GROUP BY prd_key
HAVING COUNT(*) > 1 OR prd_key IS NULL;

-- =============================================================================
-- 3. Column: prd_nm (Product Name)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50).

-- B. Check for Unwanted Spaces
-- Expectation: No result.
-- Finding: Identified records with leading/trailing whitespace.
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- C. Consistency Check (Low Cardinality)
-- Objective: Verify if product names are consistent and look for naming anomalies.
-- Finding: Looks Like OK.
SELECT DISTINCT(TRIM(prd_nm))
FROM bronze.crm_prd_info;

-- D. Check for Missing Values
-- Expectation: No result (Product names are mandatory for reporting).
-- Finding: OK.
SELECT *
FROM bronze.crm_prd_info
WHERE prd_nm IS NULL OR prd_nm = '';

/* BUSINESS RULE FOR SILVER:
- Apply TRIM() to prd_nm.
- According to the user's selection, ensure names are preserved as-is 
  after trimming for maximum descriptive clarity.
*/

-- =============================================================================
-- 4. Column: prd_cost (Product Cost)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Currently stored as VARCHAR in Bronze. 
-- Must be CAST to DECIMAL(18,2) for the Silver layer.

-- B. Data Range & Validity Check
-- Objective: Identify non-numeric values, nulls, or negative costs.
-- Finding: KO for 2 products (Negative or NULL values detected).
SELECT *
FROM bronze.crm_prd_info
WHERE CAST(prd_cost AS DECIMAL(18,2)) < 0 OR prd_cost IS NULL;

-- C. Business Rule: Handling Incomplete Data
-- Objective: Map NULL values to a default (0) to avoid calculation errors.
SELECT 
    prd_cost AS original_prd_cost,
    COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0) AS cleaned_prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL;

/* BUSINESS RULE FOR SILVER:
- Cast the VARCHAR field to DECIMAL(18,2).
- Use COALESCE to fill missing costs with 0.
*/

-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50) in Bronze. 
-- Requires cleaning and mapping to descriptive values.

-- B. Check for Unwanted Spaces
-- Expectation: No result.
-- Finding: KO (Spaces detected).
SELECT prd_line
FROM bronze.crm_prd_info
WHERE LENGTH(prd_line) != LENGTH(TRIM(prd_line));

-- C. Consistency Check (Low Cardinality)
-- Objective: Verify the distribution of coded values.
SELECT DISTINCT(TRIM(prd_line)) 
FROM bronze.crm_prd_info;

-- D. Data Normalization / Mapping
-- Business Rule: Convert short codes (M, R, S, T) into user-friendly names.
-- Missing or unexpected values are mapped to 'n/a'.
SELECT 
    DISTINCT(TRIM(prd_line)) AS original_prd_line,
    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a' END AS friendly_prd_line
FROM bronze.crm_prd_info;

/* BUSINESS RULE FOR SILVER:
- Apply TRIM() and UPPER() to ensure consistent mapping.
- Standardize labels to 'Mountain', 'Road', 'Other sales', 'Touring', or 'n/a'.
*/

-- =============================================================================
-- 6. Columns: prd_start_dt & prd_end_dt (Timeline Enrichment)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR in Bronze. 
-- Must be converted (CAST) to DATE in Silver to enable chronological analysis.

-- B. Chronological Logic Check (Invalid Date Ranges)
-- Objective: Identify records where the end date is before the start date.
-- Finding: KO (Invalid date ranges detected).
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
	CAST(prd_end_dt AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE CAST(prd_end_dt AS DATE) < CAST(prd_start_dt AS DATE);

-- C. Data Enrichment: Timeline Gap Analysis
-- Business Rule: Use LEAD() to calculate a continuous timeline. 
-- If prd_end_dt is invalid, it will be set to (Next Start Date - 1 day).
-- NOTE: Applied 'selon la sélection de l'utilisateur' for historical consistency.
WITH prd_clean AS (
    SELECT
        CAST(TRIM(prd_id) AS INTEGER) AS prd_id,
        TRIM(prd_key) AS prd_key,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(prd_end_dt AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info
)
SELECT
    *,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS calculated_end_dt
FROM prd_clean
WHERE prd_key IN ('HL-U509-R', 'HL-U509');

/* BUSINESS RULE FOR SILVER:
- Cast prd_start_dt and prd_end_dt to DATE.
- Standardize prd_end_dt using LEAD() to ensure no overlaps in product history.
*/

-- =============================================================================
-- 6. Columns: prd_start_dt & prd_end_dt
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR in Bronze. 
-- Must be converted (CAST) to DATE in Silver to enable chronological analysis.

-- B. Chronological Logic Check (Invalid Date Ranges)
-- Objective: Identify records where the end date is before the start date.
-- Finding: KO (Invalid date ranges detected).
SELECT
    CAST(TRIM(prd_id) AS INTEGER) AS prd_id,
    SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_key,
    TRIM(prd_nm) AS prd_nm,
    COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0) AS prd_cost,
    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a' END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(prd_end_dt AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE CAST(prd_end_dt AS DATE) < CAST(prd_start_dt AS DATE);

-- C. Deep Dive: Case Study on 2 Specific Products
-- Objective: Analyze the overlap issue on selected products to define a fix.
-- Finding: Overlapping timelines detected for 'HL-U509-R' and 'HL-U509'.
WITH prd_clean AS (
    SELECT
        CAST(TRIM(prd_id) AS INTEGER) AS prd_id,
        SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
        REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_key,
        TRIM(prd_nm) AS prd_nm,
        COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0) AS prd_cost,
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
             WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
             WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
             WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
             ELSE 'n/a' END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(prd_end_dt AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info
)
SELECT * FROM prd_clean
WHERE prd_key IN ('HL-U509-R', 'HL-U509') 
ORDER BY prd_id;

-- D. Data Enrichment Strategy (Corrective Logic)
-- Business Rule: If prd_end_dt is invalid or creates a gap, 
-- set prd_end_dt = (Next prd_start_dt - 1 day).
-- This ensures a seamless, non-overlapping dataset for historical analysis.
WITH prd_clean AS (
    SELECT
        CAST(TRIM(prd_id) AS INTEGER) AS prd_id,
        SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
        REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_key,
        TRIM(prd_nm) AS prd_nm,
        COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0) AS prd_cost,
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
             WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
             WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
             WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
             ELSE 'n/a' END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(prd_end_dt AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info
)
SELECT
    *,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS next_prd_start_dt
FROM prd_clean
WHERE prd_key IN ('HL-U509-R', 'HL-U509');

-- =============================================================================
-- FINAL DATA QUALITY SUMMARY: bronze.crm_prd_info
-- =============================================================================

/* IDENTIFICATION & INTEGRITY:
- Primary Key: CAST to INTEGER and ensure no NULLs to maintain table integrity.
- Referential Integrity: Identification of a missing 'cat_key' (CO_PE) in the ERP 
  and detection of unsold products compared to the Sales table.
  
CLEANING BEST PRACTICES:
- Whitespace Management: Systematic TRIM() on all VARCHAR columns.
- Financial Handling: Stored as VARCHAR in Bronze; converted to DECIMAL. 
  Missing or null costs are defaulted to 0 using COALESCE.
- Normalization: Coded 'prd_line' values are mapped to descriptive friendly names.

ENRICHMENT & TRANSFORMATION STRATEGY:
- Key Derivation: Splitting 'prd_key' to create a clean 'prd_key' and a 'cat_id'.
- Timeline Management: Using LEAD() OVER (PARTITION BY prd_key ORDER BY prd_start_dt) 
  to fix overlapping date ranges and ensure a continuous historical timeline 
  (selon la sélection de l'utilisateur).
*/

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    CAST(TRIM(prd_id) AS INTEGER) AS prd_id,
    SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,
    TRIM(prd_nm) AS prd_nm,
    COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0) AS prd_cost,
    CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a' END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    -- Data Enrichment logic for historical consistency
    LEAD(CAST(prd_start_dt AS DATE)) OVER (
        PARTITION BY prd_key 
        ORDER BY prd_start_dt
    ) - 1 AS prd_end_dt
FROM bronze.crm_prd_info;