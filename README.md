# Databricks E-Commerce Medallion Architecture
An end-to-end e-commerce data engineering project built on Databricks, implementing the Medallion Architecture using Delta Lake and Unity Catalog. The pipeline ingests raw CSV data from volumes, processes it through Bronze, Silver, and Gold layers using PySpark, and produces analytics-ready datasets for business reporting.



---

## Architecture

```text
Raw CSV Data
   ↓
Bronze Layer (Raw Delta Tables)
   ↓
Silver Layer (Cleaned & Conformed Data)
   ↓
Gold Layer (Business-Ready Dimensions & Facts)
```


## Project Structure

```text
databricks-ecommerce-medallion-architecture/
│
├── 1_medallion_processing_dim/
│   ├── 1_dim_bronze.py
│   ├── 2_dim_silver.py
│   └── 3_dim_gold.py
│
├── 2_medallion_processing_fact/
│   ├── 1_fact_bronze.py
│   ├── 2_fact_silver.py
│   └── 3_fact_gold.py
│
├── ecomm-raw-data/
│   ├── order_items/
│   │   └── order_items.csv
│   ├── brands.csv
│   ├── category.csv
│   ├── customers.csv
│   ├── date.csv
│   └── products.csv
│
├── setup/
│   └── create_catalog_and_schema.py
│
└── README.md
```

---
## Medallion Architecture

### Bronze Layer
- Ingests raw e-commerce CSV data without modification
- Preserves original schema and records for traceability
- Stored as Delta tables to enable ACID compliance and time travel

### Silver Layer
- Applies data quality rules and standardization
- Handles null values, deduplication, and data type normalization
- Produces conformed dimension and fact datasets

### Gold Layer
- Generates curated, analytics-ready tables
- Implements star schema design (dimensions and facts)
- Optimized for BI queries, reporting, and downstream analytics
---

---
## Setup

### Create Catalog & Schemas

  setup/create_catalog_and_schema.py


### Upload Raw Data

  Place CSV files under ecomm-raw-data/


### Dimensions:

  1_dim_bronze → 2_dim_silver → 3_dim_gold


### Facts:

  1_fact_bronze → 2_fact_silver → 3_fact_gold
  
---

## Tech Stack
```text
Databricks

PySpark

Delta Lake

Unity Catalog

SQL
```

## Key Highlights
```text
1. Medallion Architecture implementation

2. Delta Lake ACID transactions

3. Unity Catalog governance

4. Star schema data modeling

5. Scalable PySpark transformations
```

---
### Author

LinkedIn: http://www.linkedin.com/in/SwapnilTaware

GitHub: https://github.com/itsSwapnil

Email: tawareswapnil23@gmail.com

---


## Acknowledgements
```text
This project was built as part of my learning journey on Databricks and modern data engineering practices.  
I would like to give credit to the **Codebasics YouTube channel**, which provided clear and practical explanations of Databricks, Delta Lake, and Medallion Architecture concepts that inspired and guided this implementation.

The project structure and ideas have been adapted and extended with my own understanding and hands-on practice.
```
