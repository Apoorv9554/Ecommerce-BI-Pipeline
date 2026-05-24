# Star Schema Design

## Overview

The Gold layer of the E-commerce BI Pipeline was designed using a Star Schema model for analytical querying and dashboard optimization.

The Star Schema consists of:
- One central Fact Table
- Multiple surrounding Dimension Tables

This structure improves:
- Query performance
- Reporting efficiency
- Dashboard responsiveness
- Business analytics

---

# Star Schema Diagram

![Star Schema](architecture/star_schema.png)

---

# Fact Table

## gold_fact_order_item

The fact table stores transactional sales records.

### Measures
- quantity
- unit_price
- gross_amount
- discount_amount
- tax_amount
- net_amount
- net_amount_inr

### Keys
- customer_id
- product_id
- date_id

### Additional Attributes
- transaction_id
- transaction_ts
- channel
- coupon_flag

---

# Dimension Tables

---

## 1. Product Dimension

### Table
gld_dim_products

### Attributes
- product_id
- sku
- category
- brand
- color
- size
- material

### Purpose
Provides product-level analytical insights.

---

## 2. Customer Dimension

### Table
gld_dim_customers

### Attributes
- customer_id
- country
- state
- region

### Purpose
Supports customer segmentation and geographic analysis.

---

## 3. Date Dimension

### Table
gld_dim_date

### Attributes
- date_id
- date
- year
- quarter
- week_of_year
- day_name
- is_weekend

### Purpose
Enables time-series and trend analysis.

---

# Relationships

| Fact Table | Dimension Table | Join Key |
|---|---|---|
| gold_fact_order_item | gld_dim_products | product_id |
| gold_fact_order_item | gld_dim_customers | customer_id |
| gold_fact_order_item | gld_dim_date | date_id |

---

# Business Analytics Enabled

The Star Schema supports:
- Sales Analysis
- Customer Insights
- Product Analytics
- Financial KPIs
- Coupon & Discount Analysis
- Revenue Trends
- Geographic Analysis

---

# Dashboard Integration

The schema powers the Databricks SQL Dashboard pages:
1. Executive Summary
2. Sales Analysis
3. Customer Insights
4. Product Analytics
5. Discount & Coupon Analysis
6. Financial Analysis

---

# Advantages of Star Schema

| Feature | Benefit |
|---|---|
| Simplified Queries | Faster analytics |
| Denormalized Design | Better dashboard performance |
| BI Friendly | Easy reporting |
| Scalable | Handles large fact tables |
| Optimized Joins | Improved SQL execution |

---

# Conclusion

The Star Schema design transformed the Gold layer into a business-ready analytical model optimized for BI reporting and dashboard visualization.
