/*
============================================================================================
Database Initialization Script
============================================================================================
Project: SQL Data Warehouse Mastery
Author: Coline Plé

Description:
    This script initializes the 'sql_mastery' database. 
    It is the foundation of the Medallion Architecture (Bronze, Silver, Gold).

IMPORTANT:
    This script MUST be executed while connected to the default 'postgres' database.
    You cannot drop or create 'sql_mastery' if you are currently connected to it.

WARNING:
    This script is DESTRUCTIVE. It will drop the entire 'sql_mastery' database.
============================================================================================
*/

-- Step 1: Drop the database if it exists
-- The WITH (FORCE) option ensures we close any existing connections.
DROP DATABASE IF EXISTS sql_mastery WITH (FORCE);

-- Step 2: Create the sql_mastery database
CREATE DATABASE sql_mastery;
