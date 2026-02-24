/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates the staging tables in the 'bronze' schema.
    It drops existing tables before recreation to ensure a clean state.

Design Strategy (Schema-on-Read):
    All columns are intentionally defined as VARCHAR or TEXT. 
    This ensures that the data ingestion (Load) never fails due to data type 
    mismatches or "dirty" data in the source CSV files. 
    Data cleaning, casting, and validation are deferred to the Silver Layer.
    
Usage:
    - Ensure you are connected to the 'sql_mastery' database.
    - Run this script to define the DDL structure of the Bronze layer.
===============================================================================
*/

-- =============================================================================
-- 1. CRM Tables
-- =============================================================================

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id             VARCHAR(50),
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gender         VARCHAR(50),
    cst_create_date    VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id       VARCHAR(50),
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(100),
    prd_cost     VARCHAR(50),
    prd_line     VARCHAR(50),
    prd_start_dt VARCHAR(50),
    prd_end_dt   VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cus_id   VARCHAR(50),
    sls_ord_dt   VARCHAR(50),
    sls_ship_dt  VARCHAR(50),
    sls_due_dt   VARCHAR(50),
    sls_sales    VARCHAR(50),
    sls_quantity VARCHAR(50),
    sls_price    VARCHAR(50)
);

-- =============================================================================
-- 2. ERP Tables
-- =============================================================================

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid   VARCHAR(50),
    bdate VARCHAR(50),
    gen   VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid   VARCHAR(50),
    cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          VARCHAR(50),
    cat         VARCHAR(50),
    subcat      VARCHAR(50),
    maintenance VARCHAR(50)
);
