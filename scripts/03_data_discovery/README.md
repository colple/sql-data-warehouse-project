## 03_data_discovery - Source Data Quality Audit

This directory focuses on the exploration and profiling of the **6 source tables** from CRM and ERP systems. The goal was to identify inconsistencies before moving to the Silver layer.

### 🔍 Audit Scope:
- **CRM Tables**: Customers, Products, Sales.
- **ERP Tables**: Locations, Categories, Inventory.

### 🛠️ Identified Cleaning & Transformation Needs:
- **Standardization**: Unifying categorical values (e.g., countries, categories) to follow a single naming convention.
- **Handling Missing Values**: Identifying NULLs and defining imputation or exclusion strategies.
- **Data Enrichment**: Determining where to join tables to add business value.
- **Integrity Checks**: Detecting duplicates and validating Primary Key/Foreign Key relationships.
- **Data Harmonization**: Aligning different formats from CRM and ERP.
