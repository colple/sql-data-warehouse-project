/*
===============================================================================
Script: Run Bronze Layer Load
===============================================================================
Script Purpose:
    This script executes the data loading process for the Bronze layer.
    You can choose between two versions of the stored procedure:
    
    1. Static Version (02a): Uses paths hardcoded within the procedure.
    2. Parametric Version (02b): Uses a dynamic path passed as an argument.
===============================================================================
*/

-- =============================================================================
-- OPTION 1: Calling the Static Procedure (02a)
-- =============================================================================
-- This version is simple but requires manual path updates inside the procedure code.

-- CALL bronze.load_bronze_layer_static();


-- =============================================================================
-- OPTION 2: Calling the Parametric Procedure (02b) - RECOMMENDED
-- =============================================================================
-- This version is flexible. You pass the path to your datasets folder as a parameter.
-- Replace '/YOUR_LOCAL_PATH/' with your actual local repository path.

-- CALL bronze.load_bronze_layer('/YOUR_LOCAL_PATH/datasets');

DO $$ 
BEGIN 
    RAISE NOTICE 'Select and uncomment one of the CALL statements above to run the load.';
END $$;
