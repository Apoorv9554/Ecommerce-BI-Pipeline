# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Dashboard Table (Star Schema Ready)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

catalog = "ecommerce"

# COMMAND ----------

# MAGIC %md
# MAGIC ## # LOAD GOLD FACT + DIMENSIONS
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

fact = spark.table(f"{catalog}.gold.gold_fact_order_item")

cust = spark.table(f"{catalog}.gold.gld_dim_customers")

prod = spark.table(f"{catalog}.gold.gld_dim_products")

date_dim = spark.table(f"{catalog}.gold.gld_dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BUILD STAR SCHEMA DASHBOARD TABLE

# COMMAND ----------

df_clean = (

    fact.alias("f")

    # =========================
    # JOIN CUSTOMER DIMENSION
    # =========================
    .join(
        cust.alias("c"),
        F.col("f.customer_sk") == F.col("c.customer_sk"),
        "left"
    )

    # =========================
    # JOIN PRODUCT DIMENSION
    # =========================
    .join(
        prod.alias("p"),
        F.col("f.product_sk") == F.col("p.product_sk"),
        "left"
    )

    # =========================
    # JOIN DATE DIMENSION
    # =========================
    .join(
        date_dim.alias("d"),
        F.col("f.date_sk") == F.col("d.date_sk"),
        "left"
    )

    # =========================
    # FINAL SELECT
    # =========================
    .select(

        # =========================
        # DATE DIMENSION
        # =========================
        F.col("f.date_sk"),

        F.col("f.transaction_date"),

        F.col("f.transaction_ts"),

        F.col("d.year"),

        F.col("d.quarter"),

        F.col("d.day_name"),

        F.col("d.is_weekend"),

        # =========================
        # CUSTOMER DIMENSION
        # =========================
        F.col("f.customer_sk"),

        F.col("c.customer_id"),

        F.col("c.country"),

        F.col("c.state"),

        F.col("c.region"),

        # =========================
        # PRODUCT DIMENSION
        # =========================
        F.col("f.product_sk"),

        F.col("p.product_id"),

        F.col("p.sku"),

        F.col("p.color"),

        F.col("p.size"),

        F.col("p.material"),

        F.col("p.category_name").alias("category"),

        F.col("p.brand_name").alias("brand"),

        # =========================
        # TRANSACTION DETAILS
        # =========================
        F.col("f.transaction_id"),

        F.col("f.seq_no"),

        F.col("f.channel"),

        F.col("f.coupon_code"),

        F.col("f.coupon_flag"),

        # =========================
        # SALES METRICS
        # =========================
        F.col("f.quantity"),

        F.col("f.unit_price_currency"),

        F.col("f.unit_price"),

        F.col("f.gross_amount"),

        F.col("f.discount_percent"),

        F.col("f.discount_amount"),

        F.col("f.tax_amount"),

        F.col("f.net_amount"),

        F.col("f.net_amount_inr")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## VALIDATION CHECKS

# COMMAND ----------

print(
    "Null customer_sk:",
    df_clean.filter(F.col("customer_sk").isNull()).count()
)

print(
    "Null product_sk:",
    df_clean.filter(F.col("product_sk").isNull()).count()
)

print(
    "Null date_sk:",
    df_clean.filter(F.col("date_sk").isNull()).count()
)


# COMMAND ----------

# DISPLAY SAMPLE

display(df_clean.limit(10))

# COMMAND ----------

# =========================
# WRITE FINAL DASHBOARD TABLE
# =========================

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.sales_dashboard")

# COMMAND ----------

# =========================
# VALIDATE FINAL TABLE
# =========================

spark.sql(f"""
SELECT 
    COUNT(*) AS total_rows,
    SUM(discount_amount) AS total_discount,
    SUM(net_amount) AS total_sales,
    SUM(net_amount_inr) AS total_sales_inr
FROM {catalog}.gold.sales_dashboard
""").show()