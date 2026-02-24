/*
===============================================================================================
Stored Procedure: Load Bronze Layer (Truncate & Insert)
===============================================================================================
Script Purpose:
    This procedure loads data from CSV files into the 'bronze' schema tables.
    It uses the Truncate & Insert technique to ensure the bronze layer reflects
    the latest source data.
    
Parameters:
    None.

IMPORTANT NOTE:
    This is the STATIC version of the procedure. The file paths are hardcoded below.
    Before executing, you MUST replace '/path/to/datasets/' with the actual absolute 
    path where your CSV files are located on your local machine.

Usage:
    CALL bronze.load_bronze_layer();
===============================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze_layer()
LANGUAGE plpgsql
AS $$
DECLARE
    table_start_time    TIMESTAMP;
    table_end_time      TIMESTAMP;
    batch_start_time    TIMESTAMP;
    batch_end_time      TIMESTAMP;
    total_duration      INTERVAL;
    row_count           INTEGER;
BEGIN

    batch_start_time := clock_timestamp();

    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'LOADING BRONZE LAYER';
    RAISE NOTICE '=============================================================================';

    RAISE NOTICE ' ';

    -- ==========================================================================================
    -- CRM Tables
    -- ==========================================================================================

    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '=============================================================================';

    -- ------------------------------------------------------------------------------------------
    -- Table: crm_cust_info
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading table: bronze.crm_cust_info';

    TRUNCATE TABLE bronze.crm_cust_info;

    COPY bronze.crm_cust_info 
    FROM '/path/to/datasets/source_crm/cust_info.csv' 
    DELIMITER ',' 
    CSV HEADER;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_end_time := clock_timestamp();
    total_duration := table_end_time - table_start_time;

    RAISE NOTICE '>> Completed loading bronze.crm_cust_info: % rows inserted in % seconds', row_count, ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);

    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: crm_prd_info
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading table: bronze.crm_prd_info';

    TRUNCATE TABLE bronze.crm_prd_info;

    COPY bronze.crm_prd_info
    FROM '/path/to/datasets/source_crm/prd_info.csv' 
    DELIMITER ','
    CSV HEADER;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_end_time := clock_timestamp();
    total_duration := table_end_time - table_start_time;

    RAISE NOTICE '>> Completed loading bronze.crm_prd_info: % rows inserted in % seconds', row_count, ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);

    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: crm_sales_details
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading table: bronze.crm_sales_details';

    TRUNCATE TABLE bronze.crm_sales_details;

    COPY bronze.crm_sales_details
    FROM '/path/to/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_end_time := clock_timestamp();
    total_duration := table_end_time - table_start_time;
    RAISE NOTICE '>> Completed loading bronze.crm_sales_details: % rows inserted in % seconds', row_count, ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);

    RAISE NOTICE ' ';

    -- ==========================================================================================
    -- ERP Tables
    -- ==========================================================================================

    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '=============================================================================';

    -- ------------------------------------------------------------------------------------------
    -- Table: erp_cust_az12
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading table: bronze.erp_cust_az12';

    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/path/to/datasets/source_erp/cust_az12.csv'
    DELIMITER ','
    CSV HEADER;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_end_time := clock_timestamp();
    total_duration := table_end_time - table_start_time;
    RAISE NOTICE '>> Completed loading bronze.erp_cust_az12: % rows inserted in % seconds', row_count, ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);

    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: erp_loc_a101
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading table: bronze.erp_loc_a101';

    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/path/to/datasets/source_erp/loc_a101.csv'
    DELIMITER ','
    CSV HEADER;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_end_time := clock_timestamp();
    total_duration := table_end_time - table_start_time;
    RAISE NOTICE '>> Completed loading bronze.erp_loc_a101: % rows inserted in % seconds', row_count, ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);

    RAISE NOTICE ' ';

    -- ------------------------------------------------------------------------------------------
    -- Table: erp_px_cat_g1v2
    -- ------------------------------------------------------------------------------------------
    table_start_time := clock_timestamp();
    RAISE NOTICE '>> Loading table: bronze.erp_px_cat_g1v2';

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/path/to/datasets/source_erp/px_cat_g1v2.csv'
    DELIMITER ','
    CSV HEADER;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_end_time := clock_timestamp();
    total_duration := table_end_time - table_start_time;
    RAISE NOTICE '>> Completed loading bronze.erp_px_cat_g1v2: % rows inserted in % seconds', row_count, ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);

    RAISE NOTICE ' ';

    batch_end_time := clock_timestamp();
    total_duration := batch_end_time - batch_start_time;

    RAISE NOTICE '=============================================================================';
    RAISE NOTICE '>> Bronze Layer Loading completed successfully in % seconds', ROUND(EXTRACT(SECOND FROM total_duration)::numeric, 2);
    RAISE NOTICE '=============================================================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '=============================================================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING!';
    RAISE NOTICE 'Error State   : %', SQLSTATE;
    RAISE NOTICE 'Error Message : %', SQLERRM;
    RAISE NOTICE '============================================================================';

END;
$$;
