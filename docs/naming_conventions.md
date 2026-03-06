# **Naming Conventions**

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

## **Table of Contents**

1. [General Principles](#1-general-principles)
2. [Table Naming Conventions](#2-table-naming-conventions)
   - [Bronze Rules](#bronze-rules)
   - [Silver Rules](#silver-rules)
   - [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#3-column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedure Naming Conventions](#4-stored-procedure-naming-conventions)

---

## **1. General Principles**

- **Case Style**: Use `snake_case` (lowercase letters and underscores `_`).
- **Language**: Use English for all object and column names.
- **Avoid Reserved Words**: Do not use SQL reserved words (e.g., `SELECT`, `TABLE`, `DATE`) as object names.

---

## **2. Table Naming Conventions**

### **Bronze Rules**
- Tables must match their original source names to maintain a direct link with source systems.
- **Pattern**: `<sourcesystem>_<entity>`  
  - `<sourcesystem>`: Name of the source system (e.g., `crm`, `erp`).  
  - `<entity>`: Exact table name from the source system.  
  - Example: `crm_customer_info` → Customer information from the CRM system.

### **Silver Rules**
- Tables retain the source system prefix but undergo cleansing and standardization.
- **Pattern**: `<sourcesystem>_<entity>`  
  - `<sourcesystem>`: Name of the source system (e.g., `crm`, `erp`).  
  - `<entity>`: Exact table name from the source system.  
  - Example: `crm_customer_info` → Customer information from the CRM system.

### **Gold Rules**
- Tables use meaningful, business-aligned names. Since this layer is implemented via **Views**, names represent logical business entities.
- **Pattern**: `<category>_<entity>`  
  - `<category>`: Describes the role of the table, such as `dim` (dimension) or `fact` (fact table).  
  - `<entity>`: Descriptive name of the table, aligned with the business domain.  
  - Examples:
    - `dim_customers` → Dimension table for customer data.  
    - `fact_sales` → Fact table containing sales transactions.  

#### **Glossary of Category Patterns**

| Pattern     | Meaning                           | Status in this Project | Example(s)                              |
|-------------|-----------------------------------|-------------------------|-----------------------------------------|
| `dim_`      | Dimension table                  | **Active** | `dim_customers`, `dim_products`         |
| `fact_`     | Fact table                       | **Active** | `fact_sales`                            |
| `agg_`      | Aggregated table                 | *Not Concerned* | `agg_sales_monthly`                     |

---

## **3. Column Naming Conventions**

### **Surrogate Keys**
- All primary keys in dimension tables and their corresponding foreign keys in the fact table must use the suffix `_key`.
- **Pattern**: `<entity>_key`  
  - `<entity>`: Refers to the name of the entity the key belongs to.  
  - `_key`: A suffix indicating that this column is a surrogate key.  
  - Example: `customer_key` → Surrogate key in the `dim_customers` table.
  
### **Technical Columns**
- All technical columns must start with the prefix `dwh_`, followed by a descriptive name indicating the column's purpose.
- **Pattern**: `dwh_<column_name>`  
  - `dwh`: Prefix exclusively for system-generated metadata.  
  - Example: `dwh_insertion_date` → System-generated column used to store the date when the record was loaded.

---

## **4. Stored Procedure Naming Conventions**

- All stored procedures used for loading data must follow a task-oriented naming pattern.
- **Pattern**: `load_<layer>`
  - Examples: 
    - `load_bronze` → Stored procedure for loading data into the Bronze layer.
    - `load_silver` → Stored procedure for loading data into the Silver layer.

---