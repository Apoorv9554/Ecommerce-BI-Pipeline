# Medallion Architecture

## Overview

This project follows the Databricks Medallion Architecture approach for building scalable and maintainable data engineering pipelines.

The Medallion Architecture organizes data into multiple layers:

1. Bronze Layer
2. Silver Layer
3. Gold Layer

This layered architecture improves:
- Data quality
- Scalability
- Maintainability
- Governance
- Analytical performance

---

# Architecture Diagram

![Medallion Architecture](architecture/medallion_architecture.png)

---

# Bronze Layer

## Purpose

The Bronze layer stores raw ingested data exactly as received from source systems.

## Characteristics
- Raw CSV ingestion
- Minimal transformations
- Schema enforcement
- Metadata tracking
- Historical preservation

## Tables
- bronze.brand
- bronze.category
- bronze.customers
- bronze.products
- bronze.order_items
- bronze.dates

## Benefits
- Preserves original source data
- Enables reprocessing
- Supports data lineage

---

# Silver Layer

## Purpose

The Silver layer cleanses and standardizes raw data.

## Transformations
- Data type conversion
- Null handling
- Deduplication
- Standardization
- Validation
- Business rule application

## Examples
- Currency formatting cleanup
- Coupon normalization
- Timestamp conversion
- Product material standardization

## Tables
- silver.brand
- silver.category
- silver.customers
- silver.products
- silver.order_items
- silver.dates

## Benefits
- Improved data quality
- Consistent schemas
- Analytics-ready datasets

---

# Gold Layer

## Purpose

The Gold layer contains curated business-level tables optimized for analytics and dashboarding.

## Components

### Dimension Tables
- gld_dim_products
- gld_dim_customers
- gld_dim_date

### Fact Table
- gold_fact_order_item

### Dashboard Table
- sales_dashboard

## Business Metrics
- Total Sales
- Net Revenue
- Discounts
- Tax Analysis
- Product Performance
- Customer Insights

## Benefits
- Faster BI queries
- Simplified reporting
- Business-ready datasets

---

# Workflow Execution

The complete Medallion pipeline is automated using Databricks Jobs.

## Execution Flow

Bronze → Silver → Gold

## Features
- Scheduled execution
- Dependency management
- Automated orchestration
- Delta optimization

---

# Advantages of Medallion Architecture

| Feature | Benefit |
|---|---|
| Layered Design | Better organization |
| Data Validation | Improved quality |
| Scalability | Handles large datasets |
| Reusability | Easier development |
| Governance | Better data control |
| Analytics Ready | Faster dashboarding |

---

# Conclusion

The Medallion Architecture enabled the development of a scalable and production-style E-commerce BI pipeline using Databricks, Delta Lake, and PySpark.
