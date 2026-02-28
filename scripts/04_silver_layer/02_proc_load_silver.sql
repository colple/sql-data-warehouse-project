/*
===============================================================================================
Stored Procedure: Load Silver Layer (Truncate & Insert with Quality Gates)
===============================================================================================
Script Purpose:
    This procedure orchestrates the transformation and loading of data from the 'bronze' 
    layer to the 'silver' schema using a "Strict Quality Gate" approach.

Data Quality Strategy:
    1. Mandatory Key Validation: Records with NULL primary keys are strictly rejected.
    2. Uniqueness Enforcement: 
       - CRM: Uses ROW_NUMBER() to keep the latest version based on business dates.
       - ERP: Uses COUNT() OVER() to isolate and reject ALL rows of a duplicated ID 
         to prevent data ambiguity.
    3. Data Sanitization: Standardizes genders (Male/Female), trims whitespace, 
       cleans IDs (removing NAS prefixes or hyphens), and fixes out-of-range dates.
    4. Financial Integrity: Recalculates sales and prices in the sales table if 
       inconsistencies are detected (Sales != Qty * Price).
    5. Quarantine Logging: Every rejected row is preserved in 'silver.quality_quarantine' 
       as a JSONB object for audit and manual resolution.

Usage:
    CALL silver.load_silver_layer();
===============================================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver_layer()
LANGUAGE plpgsql
AS $$
DECLARE
    table_start_time    TIMESTAMP;
    table_end_time      TIMESTAMP;
    batch_start_time    TIMESTAMP;
    batch_end_time      TIMESTAMP;
    total_duration      INTERVAL;
    row_count           INTEGER;
    rows_bronze         INTEGER;
    rows_rejected       INTEGER;
    table_status        VARCHAR(10);

BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'LOADING SILVER LAYER (WITH QUARANTINE)';
    RAISE NOTICE '=============================================================================';
    RAISE NOTICE ' ';

    TRUNCATE TABLE silver.quality_quarantine;

    -- ==========================================================================================
    -- CRM Tables
    -- ==========================================================================================

    -- ------------------------------------------------------------------------------------------
    -- Table: crm_cust_info
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    SELECT COUNT(*) INTO rows_bronze FROM bronze.crm_cust_info;
    
    RAISE NOTICE '>> Loading table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    -- SUCCESSFUL LOAD: Process clean and unique records
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
        TRIM(cst_key)           AS cst_key,
        TRIM(cst_firstname)     AS cst_firstname,
        TRIM(cst_lastname)      AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a' 
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
            ELSE 'n/a' 
        END AS cst_gender,
        CAST(cst_create_date AS DATE) AS cst_create_date
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_duplicate
        FROM bronze.crm_cust_info 
        WHERE cst_id IS NOT NULL
    ) AS ranked_customers
    WHERE flag_duplicate = 1 AND cst_key IS NOT NULL;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    -- QUARANTINE LOAD: Capture rejected records (Duplicates and Missing Keys)
    INSERT INTO silver.quality_quarantine (
        source_table,
        rejected_column,
        rejected_reason,
        raw_data
    )
    SELECT 
        'bronze.crm_cust_info',
        'cst_id',
        'Duplicate Record',
        to_jsonb(t.*)
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_duplicate 
        FROM bronze.crm_cust_info 
        WHERE cst_id IS NOT NULL
    ) AS t 
    WHERE flag_duplicate > 1
    UNION ALL 
    SELECT 
        'bronze.crm_cust_info', 
        CASE WHEN cst_id IS NULL THEN 'cst_id' ELSE 'cst_key' END, 
        'Missing Mandatory Key', 
        to_jsonb(b.*)
    FROM bronze.crm_cust_info AS b 
    WHERE cst_id IS NULL OR cst_key IS NULL;

    -- METRICS & LOGGING: Calculate rejected rows and execution time
    rows_rejected := rows_bronze - row_count;
    table_status  := CASE WHEN rows_rejected = 0 THEN 'OK' ELSE 'KO' END;
    RAISE NOTICE '>> Table: silver.crm_cust_info | Status: [%] | Bronze: % | Silver: % | Rejected: % | Duration: %', 
                 table_status, rows_bronze, row_count, rows_rejected, (clock_timestamp() - table_start_time);
    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: crm_prd_info
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    SELECT COUNT(*) INTO rows_bronze FROM bronze.crm_prd_info;
    
    RAISE NOTICE '>> Loading table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    -- SUCCESSFUL LOAD: Only products that are strictly unique
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
        REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_')  AS cat_id,
        TRIM(prd_nm)                  AS prd_nm,
        COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0)       AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a' 
        END AS prd_line,
        CAST(prd_start_dt AS DATE)    AS prd_start_dt,
        LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY CAST(prd_start_dt AS DATE)) - 1 AS prd_end_dt
    FROM (
        SELECT *, COUNT(*) OVER (PARTITION BY prd_id) AS occurrence_count
        FROM bronze.crm_prd_info WHERE prd_id IS NOT NULL
    ) AS checked_products
    WHERE occurrence_count = 1;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    -- QUARANTINE LOAD: Capture rejected records (Duplicates and Missing Keys)
    INSERT INTO silver.quality_quarantine (
        source_table,
        rejected_column,
        rejected_reason,
        raw_data
    )
    SELECT 
        'bronze.crm_prd_info', 
        'prd_id', 
        'Duplicate ID - Manual Investigation Required', 
        to_jsonb(t.*)
    FROM (
        SELECT *, COUNT(*) OVER (PARTITION BY prd_id) AS occurrence_count 
        FROM bronze.crm_prd_info WHERE prd_id IS NOT NULL
    ) AS t 
    WHERE occurrence_count > 1
    UNION ALL
    SELECT 
        'bronze.crm_prd_info', 
        'prd_id', 
        'Missing Mandatory Key', 
        to_jsonb(b.*) 
    FROM bronze.crm_prd_info AS b WHERE prd_id IS NULL;

    -- METRICS & LOGGING: Calculate rejected rows and execution time
    rows_rejected := rows_bronze - row_count;
    table_status  := CASE WHEN rows_rejected = 0 THEN 'OK' ELSE 'KO' END;
    RAISE NOTICE '>> Table: silver.crm_prd_info | Status: [%] | Bronze: % | Silver: % | Rejected: % | Duration: %', 
                 table_status, rows_bronze, row_count, rows_rejected, (clock_timestamp() - table_start_time);
    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: crm_sales_details
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    SELECT COUNT(*) INTO rows_bronze FROM bronze.crm_sales_details;
    
    RAISE NOTICE '>> Loading table: silver.crm_sales_details';  
    TRUNCATE TABLE silver.crm_sales_details;

    -- SUCCESSFUL LOAD: Process sales records with valid order numbers
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
    FROM bronze.crm_sales_details WHERE sls_ord_num IS NOT NULL;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    -- QUARANTINE LOAD: Capture rejected records (Missing Order Number)
    INSERT INTO silver.quality_quarantine (
        source_table,
        rejected_column,
        rejected_reason,
        raw_data
    )
    SELECT 
        'bronze.crm_sales_details', 
        'sls_ord_num', 
        'Missing Mandatory Key: Order Number', 
        to_jsonb(b.*) 
    FROM bronze.crm_sales_details AS b WHERE sls_ord_num IS NULL;

    -- METRICS & LOGGING: Calculate rejected rows and execution time
    rows_rejected := rows_bronze - row_count;
    table_status  := CASE WHEN rows_rejected = 0 THEN 'OK' ELSE 'KO' END;
    RAISE NOTICE '>> Table: silver.crm_sales_details | Status: [%] | Bronze: % | Silver: % | Rejected: % | Duration: %', 
                 table_status, rows_bronze, row_count, rows_rejected, (clock_timestamp() - table_start_time);
    RAISE NOTICE ' ';

    -- ==========================================================================================
    -- ERP Tables
    -- ==========================================================================================

    -- ------------------------------------------------------------------------------------------
    -- Table: erp_cust_az12
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    SELECT COUNT(*) INTO rows_bronze FROM bronze.erp_cust_az12;
    
    RAISE NOTICE '>> Loading table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    -- SUCCESSFUL LOAD: Cleaning IDs and standardizing Gender
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        TRIM(CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END) AS cid,
        CASE 
            WHEN CAST(bdate AS DATE) > CURRENT_DATE OR CAST(bdate AS DATE) < CURRENT_DATE - INTERVAL '120 years' THEN NULL 
            ELSE CAST(bdate AS DATE) 
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
            ELSE 'n/a' 
        END AS gen
    FROM (
        SELECT *, 
               COUNT(*) OVER (PARTITION BY cid) AS occurrence_count 
        FROM bronze.erp_cust_az12 WHERE cid IS NOT NULL
    ) AS t
    WHERE occurrence_count = 1;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    -- QUARANTINE LOAD: Capture rejected records (Duplicates and Missing Keys)
    INSERT INTO silver.quality_quarantine (
        source_table,
        rejected_column,
        rejected_reason,
        raw_data
    )
    SELECT 
        'bronze.erp_cust_az12', 
        'cid', 
        'Duplicate ID - Manual Investigation Required', 
        to_jsonb(t.*)
    FROM (SELECT *, COUNT(*) OVER (PARTITION BY cid) AS occurrence_count FROM bronze.erp_cust_az12 WHERE cid IS NOT NULL) AS t WHERE occurrence_count > 1
    UNION ALL
    SELECT 
        'bronze.erp_cust_az12', 
        'cid', 
        'Missing Mandatory Key', 
        to_jsonb(b.*) 
    FROM bronze.erp_cust_az12 AS b WHERE cid IS NULL;

    -- METRICS & LOGGING: Calculate rejected rows and execution time
    rows_rejected := rows_bronze - row_count;
    table_status  := CASE WHEN rows_rejected = 0 THEN 'OK' ELSE 'KO' END;
    RAISE NOTICE '>> Table: silver.erp_cust_az12 | Status: [%] | Bronze: % | Silver: % | Rejected: % | Duration: %', 
                 table_status, rows_bronze, row_count, rows_rejected, (clock_timestamp() - table_start_time);
    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: erp_loc_a101
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    SELECT COUNT(*) INTO rows_bronze FROM bronze.erp_loc_a101;
    
    RAISE NOTICE '>> Loading table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    -- SUCCESSFUL LOAD: Cleaning IDs and Country names
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

    GET DIAGNOSTICS row_count = ROW_COUNT;

    -- QUARANTINE LOAD: Capture rejected records (Duplicates and Missing Keys)
    INSERT INTO silver.quality_quarantine (
        source_table,
        rejected_column,
        rejected_reason,
        raw_data
    )
    SELECT 
        'bronze.erp_loc_a101', 
        'cid', 
        'Duplicate ID - Manual Investigation Required', 
        to_jsonb(t.*)
    FROM (SELECT *, COUNT(*) OVER (PARTITION BY cid) AS occurrence_count FROM bronze.erp_loc_a101 WHERE cid IS NOT NULL) AS t WHERE occurrence_count > 1
    UNION ALL
    SELECT 
        'bronze.erp_loc_a101', 
        'cid', 
        'Missing Mandatory Key', 
        to_jsonb(b.*) 
    FROM bronze.erp_loc_a101 AS b WHERE cid IS NULL;

    -- METRICS & LOGGING: Calculate rejected rows and execution time
    rows_rejected := rows_bronze - row_count;
    table_status  := CASE WHEN rows_rejected = 0 THEN 'OK' ELSE 'KO' END;
    RAISE NOTICE '>> Table: silver.erp_loc_a101 | Status: [%] | Bronze: % | Silver: % | Rejected: % | Duration: %', 
                 table_status, rows_bronze, row_count, rows_rejected, (clock_timestamp() - table_start_time);
    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: erp_px_cat_g1v2
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    SELECT COUNT(*) INTO rows_bronze FROM bronze.erp_px_cat_g1v2;
    
    RAISE NOTICE '>> Loading table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    -- SUCCESSFUL LOAD: Trimming IDs and Category descriptions
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
    FROM (
        SELECT *, 
               COUNT(*) OVER (PARTITION BY id) AS occurrence_count 
        FROM bronze.erp_px_cat_g1v2 WHERE id IS NOT NULL
    ) AS t
    WHERE occurrence_count = 1;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    -- QUARANTINE LOAD: Capture rejected records (Duplicates and Missing Keys)
    INSERT INTO silver.quality_quarantine (
        source_table,
        rejected_column,
        rejected_reason,
        raw_data
    )
    SELECT 
        'bronze.erp_px_cat_g1v2', 
        'id', 
        'Duplicate ID - Manual Investigation Required', 
        to_jsonb(t.*)
    FROM (SELECT *, COUNT(*) OVER (PARTITION BY id) AS occurrence_count FROM bronze.erp_px_cat_g1v2 WHERE id IS NOT NULL) AS t WHERE occurrence_count > 1
    UNION ALL
    SELECT 
        'bronze.erp_px_cat_g1v2', 
        'id', 
        'Missing Mandatory Key', 
        to_jsonb(b.*) 
    FROM bronze.erp_px_cat_g1v2 AS b WHERE id IS NULL;

    -- METRICS & LOGGING: Calculate rejected rows and execution time
    rows_rejected := rows_bronze - row_count;
    table_status  := CASE WHEN rows_rejected = 0 THEN 'OK' ELSE 'KO' END;
    RAISE NOTICE '>> Table: silver.erp_px_cat_g1v2 | Status: [%] | Bronze: % | Silver: % | Rejected: % | Duration: %', 
                 table_status, rows_bronze, row_count, rows_rejected, (clock_timestamp() - table_start_time);
    RAISE NOTICE ' ';

    -- ==========================================================================================
    -- FINAL BATCH METRICS
    -- ==========================================================================================
    batch_end_time := clock_timestamp();
    total_duration := batch_end_time - batch_start_time;

    RAISE NOTICE '=============================================================================';
    RAISE NOTICE '>> Silver Layer Loading completed successfully in % seconds', ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);
    RAISE NOTICE '=============================================================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING!';
    RAISE NOTICE 'Error State   : %', SQLSTATE;
    RAISE NOTICE 'Error Message : %', SQLERRM;
    RAISE NOTICE '============================================================================';
END;
$$;