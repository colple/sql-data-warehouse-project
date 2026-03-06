# Data Catalog for Gold Layer

### 1. **gold.dim_customers**
- **Purpose:** Stores customer details enriched with demographic data from both CRM and ERP systems.
- **Grain:** One row per customer.

| Target Column | Data Type | Description | Source Table | Source Column | Transformation Rule |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **customer_key** | INTEGER | Unique surrogate key (PK). | *Generated* | N/A | `ROW_NUMBER() OVER (ORDER BY cst_id)` |
| **customer_id** | INTEGER | Original customer identifier. | silver.crm_cust_info | `cst_id` | N/A |
| **customer_number** | VARCHAR(50) | Original alphanumeric ID. | silver.crm_cust_info | `cst_key` | `TRIM(cst_key)` |
| **first_name** | VARCHAR(50) | Customer's first name. | silver.crm_cust_info | `cst_firstname` | `TRIM(cst_firstname)` |
| **last_name** | VARCHAR(50) | Customer's last name. | silver.crm_cust_info | `cst_lastname` | `TRIM(cst_lastname)` |
| **country** | VARCHAR(50) | Standardized country name. | silver.erp_loc_a101 | `cntry` | `CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States' WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany' WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a' ELSE cntry END` |
| **marital_status** | VARCHAR(50) | Standardized marital status. | silver.crm_cust_info | `cst_marital_status`| `CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' ELSE 'n/a' END` |
| **gender** | VARCHAR(50) | Consolidated gender. | CRM & ERP | `cst_gender`, `gen` | `CASE WHEN cst_gender != 'n/a' THEN cst_gender ELSE COALESCE(gen, 'n/a') END` (Standardized to Male/Female/n/a) |
| **birthdate** | DATE | Validated birthdate. | silver.erp_cust_az12 | `bdate` | `CASE WHEN CAST(bdate AS DATE) > CURRENT_DATE OR CAST(bdate AS DATE) < CURRENT_DATE - INTERVAL '120 years' THEN NULL ELSE CAST(bdate AS DATE) END` |
| **create_date** | DATE | System creation date. | silver.crm_cust_info | `cst_create_date` | N/A |

---

### 2. **gold.dim_products**
- **Purpose:** Provides a consolidated view of products, enriched with categories and maintenance info from the ERP system.
- **Grain:** One row per product.

| Target Column | Data Type | Description | Source Table | Source Column | Transformation Rule |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **product_key** | INTEGER | Unique surrogate key (PK). | *Generated* | N/A | `ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key)` |
| **product_id** | INTEGER | Original product identifier. | silver.crm_prd_info | `prd_id` | `CAST(TRIM(prd_id) AS INTEGER)` |
| **product_number** | VARCHAR(50) | Cleaned alphanumeric ID. | silver.crm_prd_info | `prd_key` | `SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key)))` |
| **product_name** | VARCHAR(100) | Full descriptive product name. | silver.crm_prd_info | `prd_nm` | `TRIM(prd_nm)` |
| **category_id** | VARCHAR(50) | Link to high-level classification. | silver.crm_prd_info | `prd_key` | `REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_')` |
| **category** | VARCHAR(50) | High-level classification. | silver.erp_px_cat_g1v2 | `cat` | `TRIM(cat)` |
| **subcategory** | VARCHAR(50) | Detailed classification. | silver.erp_px_cat_g1v2 | `subcat` | `TRIM(subcat)` |
| **maintenance** | VARCHAR(50) | Maintenance requirement flag. | silver.erp_px_cat_g1v2 | `maintenance` | `TRIM(maintenance)` |
| **cost** | DECIMAL(18,2) | Product base cost. | silver.crm_prd_info | `prd_cost` | `COALESCE(CAST(prd_cost AS DECIMAL(18,2)), 0)` |
| **product_line** | VARCHAR(50) | Standardized product series. | silver.crm_prd_info | `prd_line` | `CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales' WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' ELSE 'n/a' END` |
| **start_date** | DATE | Availability start date. | silver.crm_prd_info | `prd_start_dt` | `CAST(prd_start_dt AS DATE)` |

---

### 3. **gold.fact_sales**
- **Purpose:** Stores transactional sales data for analytical purposes, linking products and customers.
- **Grain:** One row per sales order line item.

| Target Column | Data Type | Description | Source Table | Source Column | Transformation Rule |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **order_number** | VARCHAR(50) | Unique identifier for each sales order. | silver.crm_sales_details | `sls_ord_num` | `TRIM(sls_ord_num)` |
| **product_key** | INTEGER | Surrogate key linking to product dimension. | gold.dim_products | `product_key` | `JOIN on silver.crm_sales_details.sls_prd_key = gold.dim_products.product_number` |
| **customer_key** | INTEGER | Surrogate key linking to customer dimension. | gold.dim_customers | `customer_key` | `JOIN on silver.crm_sales_details.sls_cus_id = gold.dim_customers.customer_id` |
| **order_date** | DATE | Date the order was placed. | silver.crm_sales_details | `sls_ord_dt` | `CASE WHEN LENGTH(sls_ord_dt) != 8 OR sls_ord_dt = '0' THEN NULL ELSE CAST(sls_ord_dt AS DATE) END` |
| **shipping_date** | DATE | Date the order was shipped. | silver.crm_sales_details | `sls_ship_dt` | `CASE WHEN LENGTH(sls_ship_dt) != 8 OR sls_ship_dt = '0' THEN NULL ELSE CAST(sls_ship_dt AS DATE) END` |
| **due_date** | DATE | Due date for the order. | silver.crm_sales_details | `sls_due_dt` | `CASE WHEN LENGTH(sls_due_dt) != 8 OR sls_due_dt = '0' THEN NULL ELSE CAST(sls_due_dt AS DATE) END` |
| **sales_amount** | DECIMAL(18,2) | Total revenue for the line item. | silver.crm_sales_details | `sls_sales` | `CASE WHEN sls_sales IS NULL OR sls_sales != sls_quantity * sls_price THEN sls_quantity * sls_price ELSE sls_sales END` |
| **quantity** | INTEGER | Number of units ordered. | silver.crm_sales_details | `sls_quantity` | `CAST(sls_quantity AS INTEGER)` |
| **price** | DECIMAL(18,2) | Unit price (recalculated if missing or zero). | silver.crm_sales_details | `sls_price` | `CASE WHEN sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END` |

---

> **Note:** This Data Catalog is designed to serve as the technical "Source of Truth." The transformation rules listed here (according to the user's selection) are strictly aligned with the Gold layer SQL scripts to ensure total integrity between the documentation and the deployed code.