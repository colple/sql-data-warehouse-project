/*
===============================================================================
Data Quality Discovery: bronze.crm_sales_details
===============================================================================
Script Purpose:
    - Audit the Sales transactions table for technical and logical integrity.
    - Validate date formats and chronological consistency (Order vs Ship vs Due).
    - Ensure mathematical consistency between Sales, Quantity, and Price.

Discovery Areas for this Table:
    1. Referential Integrity: Validating keys against Product and Customer masters.
    2. Date Cleaning: Handling invalid '0' values or incorrect formats in VARCHAR dates.
    3. Business Logic: Recalculating Sales or Price when discrepancies are found.
    4. Outlier Detection: Identifying future dates or unrealistic transaction values.

Cleaning Strategy:
    - Systematic TRIM() on all keys and identifiers.
    - NULL handling for invalid dates (Length != 8 or Value = 0).
    - Derived logic for Sales and Price to fix calculation bugs in the source.
===============================================================================
*/

-- Initial Inspection
SELECT * FROM bronze.crm_sales_details LIMIT 100;

-- =============================================================================
-- 1. Column: sls_ord_num (Order Number)
-- =============================================================================
-- A. Data Type Assessment: VARCHAR(50)

-- B. Check for Unwanted Spaces
-- Expectation: No Result.
SELECT sls_ord_num FROM bronze.crm_sales_details WHERE sls_ord_num != TRIM(sls_ord_num);

-- =============================================================================
-- 2. Column: sls_prd_key (Product Key - Join Key)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR(50). Essential for joining with crm_prd_info.

-- B. Check for Unwanted Spaces
-- Expectation: No result.
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE LENGTH(sls_prd_key) != LENGTH(TRIM(sls_prd_key));

-- C. Check for Nulls
-- Expectation: No result.
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key IS NULL OR sls_prd_key = '';

-- D. Referential Integrity Check (with crm_prd_info)
-- Objective: Ensure every product sold exists in the product master table.
-- Expectation: No Result.
SELECT DISTINCT sls_prd_key
FROM bronze.crm_sales_details
WHERE TRIM(sls_prd_key) NOT IN (
    SELECT DISTINCT SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key)))
    FROM bronze.crm_prd_info
);

/* BUSINESS RULE FOR SILVER:
- Apply TRIM() to ensure keys are perfectly aligned for future joins.
*/

-- =============================================================================
-- 3. Column: sls_cus_id (Customer ID - Join Key)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Current data is stored as VARCHAR in Bronze. 
-- Must be CAST to INTEGER during the Silver layer load.

-- B. Check for Unwanted Spaces
-- Expectation: No result.
SELECT sls_cus_id
FROM bronze.crm_sales_details
WHERE LENGTH(sls_cus_id) != LENGTH(TRIM(sls_cus_id));

-- C. Check for Nulls or Empty Strings
-- Expectation: No result.
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cus_id IS NULL OR sls_cus_id = '';

-- D. Referential Integrity Check (with crm_cust_info)
-- Objective: Ensure every sale is linked to a valid customer.
-- Expectation: No result.
SELECT DISTINCT sls_cus_id
FROM bronze.crm_sales_details
WHERE TRIM(sls_cus_id) NOT IN (
    SELECT DISTINCT SUBSTRING(TRIM(cst_id), 1, 5)
    FROM bronze.crm_cust_info
);

-- =============================================================================
-- 4. Columns: sls_ord_dt, sls_ship_dt, sls_due_dt (Dates)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR in Bronze. 
-- Must be converted to DATE.

-- B. Format & Validity Check (YYYYMMDD)
-- Objective: Identify invalid lengths, '0' values, or negative numbers.
-- Finding: KO (Negative numbers and '0' values detected).
SELECT *
FROM bronze.crm_sales_details
WHERE LENGTH(TRIM(sls_ord_dt)) != 8 
   OR CAST(sls_ord_dt AS INTEGER) <= 0;

-- C. Outlier Detection (Future dates or far past)
-- Finding: Identification of unrealistic dates.
WITH cleaned_data AS (
    SELECT
        CASE WHEN TRIM(sls_ord_dt) = '0' OR LENGTH(TRIM(sls_ord_dt)) != 8 THEN NULL 
             ELSE CAST(sls_ord_dt AS DATE) END AS sls_ord_dt,
        CASE WHEN TRIM(sls_ship_dt) = '0' OR LENGTH(TRIM(sls_ship_dt)) != 8 THEN NULL 
             ELSE CAST(sls_ship_dt AS DATE) END AS sls_ship_dt,
        CASE WHEN TRIM(sls_due_dt) = '0' OR LENGTH(TRIM(sls_due_dt)) != 8 THEN NULL 
             ELSE CAST(sls_due_dt AS DATE) END AS sls_due_dt
    FROM bronze.crm_sales_details
)
SELECT * 
FROM cleaned_data
WHERE (sls_ord_dt > CURRENT_DATE OR sls_ord_dt < '2000-01-01')
   OR (sls_ship_dt > CURRENT_DATE OR sls_ship_dt < '2000-01-01')
   OR (sls_due_dt > (CURRENT_DATE + INTERVAL '1 month') OR sls_due_dt < '2000-01-01');

-- D. Chronological Sequence Check
-- Objective: Ensure Order <= Ship and Order <= Due.
-- Finding: OK (Expectation: No Result).
WITH cleaned_data AS (
    SELECT
        CASE WHEN TRIM(sls_ord_dt) = '0' OR LENGTH(TRIM(sls_ord_dt)) != 8 THEN NULL 
             ELSE CAST(sls_ord_dt AS DATE) END AS sls_ord_dt,
        CASE WHEN TRIM(sls_ship_dt) = '0' OR LENGTH(TRIM(sls_ship_dt)) != 8 THEN NULL 
             ELSE CAST(sls_ship_dt AS DATE) END AS sls_ship_dt,
        CASE WHEN TRIM(sls_due_dt) = '0' OR LENGTH(TRIM(sls_due_dt)) != 8 THEN NULL 
             ELSE CAST(sls_due_dt AS DATE) END AS sls_due_dt
    FROM bronze.crm_sales_details
)
SELECT * FROM cleaned_data
WHERE sls_ord_dt > sls_ship_dt 
   OR sls_ord_dt > sls_due_dt;

-- =============================================================================
-- 5. Column: sls_sales (Sales Amount)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR. 
-- Must be CAST to DECIMAL(18,2) in Silver.

-- B. Check for Unwanted Spaces
-- Finding: OK.
SELECT sls_sales
FROM bronze.crm_sales_details
WHERE LENGTH(sls_sales) != LENGTH(TRIM(sls_sales));

-- C. Mathematical Consistency Check
-- Rule: Sales amount should be equal to quantity * price.
-- Finding: KO (Records where sales is NULL, 0, or inconsistent with Qty * Price).
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL 
    OR CAST(sls_sales AS DECIMAL(18,2)) = 0
    OR CAST(sls_sales AS DECIMAL(18,2)) != CAST(sls_quantity AS DECIMAL) * CAST(sls_price AS DECIMAL);

-- D. CORRECTIVE LOGIC (Fixing the Bug)
-- Test de ton code exact pour valider le fix avant l'insertion en Silver
WITH clean AS (
SELECT
    CASE WHEN sls_sales IS NULL OR CAST(sls_sales AS DECIMAL(18,2)) != CAST(sls_quantity AS INTEGER) * CAST(sls_price AS DECIMAL(18,2)) 
        THEN CAST(sls_quantity AS INTEGER) * CAST(sls_price AS DECIMAL(18,2)) 
    ELSE CAST(sls_sales AS DECIMAL(18,2)) END AS sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
)
SELECT *
FROM clean
WHERE sls_sales IS NULL 
    OR CAST(sls_sales AS DECIMAL(18,2)) != CAST(sls_quantity AS DECIMAL(18,2)) * CAST(sls_price AS DECIMAL(18,2));

/* BUSINESS RULE FOR SILVER:
- Re-calculate 'sls_sales' as (Quantity * Price) if the source value is 
  inconsistent, ensuring we correctly capture returns (negative amounts).
*/

-- =============================================================================
-- 6. Column: sls_quantity (Quantity Sold)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR. 
-- Must be CAST to INTEGER.

-- B. Validity Check (Nulls & Logical Values)
-- Objective: Find missing values or records with 0 quantity.
-- Expectation: No Result.
SELECT *
FROM bronze.crm_sales_details
WHERE CAST(sls_quantity AS DECIMAL(18,2)) IS NULL
   OR CAST(sls_quantity AS DECIMAL(18,2)) <= 0;

-- =============================================================================
-- 7. Column: sls_price (Unit Price)
-- =============================================================================

-- A. Data Type Assessment
-- NOTE: Stored as VARCHAR. 
-- Must be CAST to DECIMAL(18,2).

-- B. Check for Unwanted Spaces
-- Expectation: No Result.
SELECT sls_price
FROM bronze.crm_sales_details
WHERE LENGTH(sls_price) != LENGTH(TRIM(sls_price));

-- C. Check for Nulls or Zero Price
-- Expectation: No Result.
SELECT *
FROM bronze.crm_sales_details
WHERE sls_price IS NULL 
    OR CAST(sls_price AS DECIMAL(18,2)) = 0;

-- D. CORRECTIVE LOGIC (Fixing the Price Bug)
-- Objective: Validate the logic before Silver load.
-- Rule: If Price is missing/zero, derive it from Sales / Quantity.
-- NOTE: Using NULLIF to prevent 'Division by zero' error.
SELECT 
    sls_price AS original_price,
    sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR CAST(sls_price AS DECIMAL(18,2)) = 0 
         THEN CAST(sls_sales AS DECIMAL(18,2)) / NULLIF(CAST(sls_quantity AS INTEGER), 0)
         ELSE CAST(sls_price AS DECIMAL(18,2)) 
    END AS fixed_price
FROM bronze.crm_sales_details
WHERE sls_price IS NULL OR CAST(sls_price AS DECIMAL(18,2)) = 0;

/* BUSINESS RULE FOR SILVER:
- Cast sls_quantity to INTEGER.
- According to the user's selection, if sls_price is invalid, derive it 
  from (sls_sales / sls_quantity) to ensure data consistency.
*/

 CASE WHEN sls_price IS NULL OR CAST(sls_price AS DECIMAL(18,2)) = 0 
        THEN CAST(sls_sales AS DECIMAL(18,2)) / NULLIF(CAST(sls_quantity AS INTEGER), 0)
    ELSE CAST(sls_price AS DECIMAL(18,2)) END AS sls_price
	

-- =============================================================================
-- FINAL DATA QUALITY SUMMARY: bronze.crm_sales_details
-- =============================================================================

/* IDENTIFICATION & INTEGRITY:
- Primary Key: Validation of 'sls_ord_num'. Records with NULL identifiers are 
  excluded to ensure every transaction is traceable.
- Referential Integrity: Keys (Product and Customer) have been verified against 
  the master tables. All keys are TRIMMED to ensure perfect joins.
  
CLEANING BEST PRACTICES:
- Whitespace Management: Systematic TRIM() applied to all VARCHAR columns (keys and order numbers).
- Date Standardization: Handled invalid '0' values and incorrect string lengths (Length != 8). 
  Dates are CAST to DATE only after format validation.
  
ENRICHMENT & TRANSFORMATION STRATEGY:
- Financial Self-Healing: Selon la sélection de l'utilisateur, a cross-validation 
  logic is applied to financial columns:
    1. If Sales is inconsistent with (Qty * Price), it is recalculated.
    2. If Price is missing or zero, it is derived from (Sales / Quantity) using NULLIF.
- This strategy ensures mathematical consistency even when the source CRM 
  data is partially corrupted or incomplete.
*/

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cus_id,
    sls_ord_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    TRIM(sls_ord_num) AS sls_ord_num,
    TRIM(sls_prd_key) AS sls_prd_key,
    CAST(TRIM(sls_cus_id) AS INTEGER) AS sls_cus_id,
    CASE 
        WHEN TRIM(sls_ord_dt) = '0' OR LENGTH(TRIM(sls_ord_dt)) != 8 THEN NULL 
        ELSE CAST(sls_ord_dt AS DATE) 
    END AS sls_ord_dt,
    CASE 
        WHEN TRIM(sls_ship_dt) = '0' OR LENGTH(TRIM(sls_ship_dt)) != 8 THEN NULL 
        ELSE CAST(sls_ship_dt AS DATE) 
    END AS sls_ship_dt,
    CASE 
        WHEN TRIM(sls_due_dt) = '0' OR LENGTH(TRIM(sls_due_dt)) != 8 THEN NULL 
        ELSE CAST(sls_due_dt AS DATE) 
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR CAST(sls_sales AS DECIMAL(18,2)) != CAST(sls_quantity AS INTEGER) * CAST(sls_price AS DECIMAL(18,2)) 
        THEN CAST(sls_quantity AS INTEGER) * CAST(sls_price AS DECIMAL(18,2)) 
        ELSE CAST(sls_sales AS DECIMAL(18,2)) 
    END AS sls_sales,
    CAST(sls_quantity AS INTEGER) AS sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR CAST(sls_price AS DECIMAL(18,2)) = 0 
        THEN CAST(sls_sales AS DECIMAL(18,2)) / NULLIF(CAST(sls_quantity AS INTEGER), 0) 
        ELSE CAST(sls_price AS DECIMAL(18,2)) 
    END AS sls_price
FROM bronze.crm_sales_details 
WHERE sls_ord_num IS NOT NULL;
