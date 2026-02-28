/*
============================================================================================
Schema Setup (Medallion Architecture)
============================================================================================
Script Purpose:
    This script sets up the medallion layers (Schemas) within the 'sql_mastery' database:
    'bronze', 'silver', and 'gold'.

IMPORTANT:
    You MUST be connected to the 'sql_mastery' database before running this script.
*/

-- Step 1: Create Bronze Layer (Raw data)
CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS 'Bronze layer for raw data directly from sources';

-- Step 2: Create Silver Layer (Cleaned data)
CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS 'Silver layer for cleaned and transformed data';

-- Step 3: Create Gold Layer (Business-ready data)
CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS 'Gold layer for aggregated and business-ready metrics';