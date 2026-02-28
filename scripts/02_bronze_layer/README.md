## 02_bronze_layer - Raw Data Ingestion
This directory contains scripts for loading source data (CSV) into the Bronze schema.

- **01_ddl_bronze.sql**: Creation of raw staging tables.
- **02a_proc_load_bronze_static**: Simple stored procedure for data loading.
- **02b_proc_load_bronze_param**: Dynamic stored procedure with parameters for flexible ingestion.
- **03_run_bronze_layer**: Global execution script to run the entire Bronze pipeline.
