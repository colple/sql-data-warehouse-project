## 05_gold_layer - Business Presentation Layer (Star Schema)

The Gold layer is the final stage of our Medallion architecture. It transforms cleaned Silver data into a structured **Star Schema** designed for high-performance BI reporting and analytical queries.

---

### 🏗️ Data Model Structure

Our presentation layer is built around a central Fact table and supporting Dimension tables:

* **gold.dim_customers**: A unified "Golden Record" of customers (CRM + ERP) with consolidated gender logic.
* **gold.dim_products**: A current catalog of active products, enriched with category and sub-category metadata.
* **gold.fact_sales**: The central transactional table linking sales to dimensions via optimized Surrogate Keys.

---

### 🚀 Key Engineering Features

* **Surrogate Keys**: Implementation of `ROW_NUMBER()` to create system-independent identifiers, ensuring stability for long-term data warehousing.
* **Data Integrity**: Integrated Foreign Key (FK) validation scripts to guarantee zero orphan records between facts and dimensions.
* **Business Rules**: 
    * *Active Catalog*: Automatic filtering of historical/obsolete product records.
    * *Master Source Logic*: CRM-first attribution for sensitive attributes like Gender.
