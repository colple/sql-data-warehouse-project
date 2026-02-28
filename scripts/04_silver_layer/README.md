## 04_silver_layer - Data Transformation & Cleaning

This layer transforms raw data from Bronze into standardized, cleaned, and enriched datasets.

- **01_ddl_silver.sql**: Defines the Silver schema structure with optimized data types.
- **02_proc_load_silver.sql**: Stored procedures for data cleaning (handling NULLs, duplicates, and standardization).
- **03_run_silver_layer.sql**: Master script to execute the Silver pipeline.

### Key Transformations:
- **Standardization**: Unified country names and category labels.
- **Data Integrity**: Removed duplicates and enforced referential integrity.
- **Handling Missing Values**: Applied imputation logic for critical fields.
- **Data Harmonization**: Cleaned prefixes and unified date formats.
