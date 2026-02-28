/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates the silver tables in the 'silver' schema.
    It drops existing tables before recreation to ensure a clean state.

Design Strategy (Schema-on-Read):
    - Clean & standardized data to prepare for analysis and reporting.
    - Data cleaning, Data standardization, Data Normalization.
    - Add Metadata Columns (dwh_insertion_date) to track data lineage.
    
Data Quality & Quarantine Strategy:
    - A dedicated 'quality_quarantine' table is created to capture records 
      that fail validation rules (e.g., Duplicate IDs, Null Keys).
    - This prevents the pipeline from breaking while allowing engineers to 
      investigate "dirty" data without stopping the entire load.
    - Using JSONB for raw data allows flexibility to store any source row format.

Foreign Keys Strategy:
    - Foreign keys are intentionally omitted at this stage. 
    - In Data Engineering, we first load data into Silver to clean it.
    - Applying FKs now would cause load failures if a child record exists 
      without a parent record in the source.
===============================================================================
*/

-- =============================================================================
-- DROP TABLES (Order matters: Drop tables that would have FKs first)
-- =============================================================================

DROP TABLE IF EXISTS silver.crm_sales_details; 
DROP TABLE IF EXISTS silver.crm_cust_info;
DROP TABLE IF EXISTS silver.crm_prd_info;
DROP TABLE IF EXISTS silver.erp_cust_az12;
DROP TABLE IF EXISTS silver.erp_loc_a101;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
DROP TABLE IF EXISTS silver.quality_quarantine;

-- =============================================================================
-- 1. CRM Tables
-- =============================================================================

CREATE TABLE silver.crm_cust_info (
    -- Keys & Identifiers
    cst_id             INTEGER NOT NULL,
    cst_key            VARCHAR(50) NOT NULL,
    -- Personal Information
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gender         VARCHAR(50),
    cst_create_date    DATE,
    -- Metadata
    dwh_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT pk_silver_crm_cust_info PRIMARY KEY (cst_id),
    CONSTRAINT uq_silver_crm_cust_info_cst_key UNIQUE (cst_key)
);

CREATE TABLE silver.crm_prd_info (
    -- Keys & Identifiers
    prd_id             INTEGER NOT NULL,
    prd_key            VARCHAR(50) NOT NULL,
    cat_id             VARCHAR(50) NOT NULL,
    -- Product Information
    prd_nm             VARCHAR(100),
    prd_cost           DECIMAL(18,2),
    prd_line           VARCHAR(50),
    prd_start_dt       DATE,
    prd_end_dt         DATE,
    -- Metadata
    dwh_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT pk_silver_crm_prd_info PRIMARY KEY (prd_id)
);

CREATE TABLE silver.crm_sales_details (
    -- Keys & Identifiers
    sls_ord_num        VARCHAR(50) NOT NULL,
    sls_prd_key        VARCHAR(50) NOT NULL,
    sls_cus_id         INTEGER NOT NULL,
    -- Sales Information
    sls_ord_dt         DATE,
    sls_ship_dt        DATE,
    sls_due_dt       DATE,
    sls_sales          DECIMAL(18,2),
    sls_quantity       INTEGER,
    sls_price          DECIMAL(18,2),
    -- Metadata
    dwh_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- Constraints: FKs will be validated post-loading to avoid ingestion blocking.
);

-- =============================================================================
-- 2. ERP Tables
-- =============================================================================

CREATE TABLE silver.erp_cust_az12 (
    -- Keys & Identifiers
    cid                VARCHAR(50) NOT NULL,
    -- Customer Information
    bdate              DATE,
    gen                VARCHAR(50),
    -- Metadata
    dwh_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT pk_silver_erp_cust_az12 PRIMARY KEY (cid)
);

CREATE TABLE silver.erp_loc_a101 (
    -- Keys & Identifiers
    cid                VARCHAR(50) NOT NULL,
    -- Location Information
    cntry              VARCHAR(50),
    -- Metadata
    dwh_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT pk_silver_erp_loc_a101 PRIMARY KEY (cid)
);

CREATE TABLE silver.erp_px_cat_g1v2 (
    -- Keys & Identifiers
    id                 VARCHAR(50) NOT NULL,
    -- Product Category Information
    cat                VARCHAR(50),
    subcat             VARCHAR(50),
    maintenance        VARCHAR(50),
    -- Metadata
    dwh_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT pk_silver_erp_px_cat_g1v2 PRIMARY KEY (id)
);

-- =============================================================================
-- 3. Data Quality & Quarantine
-- =============================================================================

CREATE TABLE silver.quality_quarantine (
    quarantine_id      SERIAL PRIMARY KEY,
    source_table       VARCHAR(50),
    rejected_column    VARCHAR(50),
    rejected_reason    VARCHAR(250),
    raw_data           JSONB,
    dwh_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.quality_quarantine IS 'Stores records that failed silver layer validation rules.';