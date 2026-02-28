/*
===============================================================================
Script: Run Silver Layer Load & Quality Audit
===============================================================================
Script Purpose:
    This script executes the data transformation and loading process from 
    'bronze' to 'silver' layer and provides tools to audit data quality.

Execution Model:
    - The procedure uses a "Strict Quality Gate" approach.
    - Clean records are moved to the 'silver' schema.
    - Corrupted or duplicate records are isolated in 'silver.quality_quarantine'.
===============================================================================
*/

-- =============================================================================
-- STEP 1: Execute the Silver Load Procedure
-- =============================================================================
-- This will truncate silver tables, transform data, and populate the quarantine.

CALL silver.load_silver_layer();

-- =============================================================================
-- STEP 2: Quick Quality Audit (Summary)
-- =============================================================================
-- Run this query to see a high-level overview of rejected records by reason.

SELECT 
    source_table, 
    rejected_reason, 
    COUNT(*) AS total_rejected_rows
FROM silver.quality_quarantine
GROUP BY source_table, rejected_reason
ORDER BY source_table;

-- =============================================================================
-- STEP 3: Detailed Investigation (Raw Data)
-- =============================================================================
-- Run this query to inspect the actual values that failed the quality rules.
-- The 'raw_data' column contains the full original row in JSONB format.

-- SELECT * FROM silver.quality_quarantine WHERE source_table = 'bronze.crm_cust_info';
-- SELECT * FROM silver.quality_quarantine WHERE rejected_reason LIKE '%Duplicate%';

DO $$ 
BEGIN 
    RAISE NOTICE 'Silver Layer execution complete. Check the Messages tab for metrics and run Step 2 to audit rejections.';
END $$;