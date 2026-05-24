# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold: Building BI Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

df = spark.table(f"{catalog_name}.silver.order_items")
df.limit(10).display()


# COMMAND ----------

# 1) Add gross amount
df = df.withColumn('gross_amount', F.col('unit_price') * F.col('quantity'))

# 2) Add discount_amount (discount_pct is already numeric, e.g., 21 -> 21%)
df = df.withColumn('discount_amount', (F.col("gross_amount") * F.col("discount_pct") / 100))

# 3) Add sale_amount (gross_amount - discount_amount)
df = df.withColumn("sale_amount", F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))

# add date id
df = df .withColumn('date_id', F.date_format(F.col('dt'),"yyyyMMdd").cast(IntegerType()))

#coupan flag
#coupan flag = 1 if coupan_code is not null else 0
df = df.withColumn(
    'coupon_flag',
    F.when(
        F.col('coupon_code').isNotNull(),
        F.lit(1)
    ).otherwise(F.lit(0))
)

df.limit(5).display()

# COMMAND ----------

fx_rates = {
    "INR" : 1.00,
    "AED" : 24.18,
    "AUD" : 57.55,
    "CAD" : 62.93,
    "GBP" : 117.98,
    "SGD" : 68.18,
    "USD" : 88.29,
}
rates = [(k,float(v)) for k,v in fx_rates.items()]
rates_df = spark.createDataFrame(rates, ["currency","inr_rate"])
rates_df.show()

# COMMAND ----------

df = (
    df
    .join(
        rates_df,
        F.upper(F.trim(F.col("unit_price_currency"))) == rates_df.currency,
        "left"
    )
    .withColumn("inr_rate", F.coalesce(F.col("inr_rate"), F.lit(1.0)))
    .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))
    .withColumn("sale_amount_inr", F.round(F.col("sale_amount_inr"), 2))
)

# COMMAND ----------

display(df.limit(5))


# COMMAND ----------

type(df)

# COMMAND ----------

orders_gold_df =  df.select(
    F.col("date_sk"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_sk"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_sk"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency").alias("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)
 

# COMMAND ----------

print("Null product_sk:", orders_gold_df.filter(F.col("product_sk").isNull()).count())
print("Null customer_sk:", orders_gold_df.filter(F.col("customer_sk").isNull()).count())
print("Null date_sk:", orders_gold_df.filter(F.col("date_sk").isNull()).count())

# COMMAND ----------

#write raw data to gold layer
orders_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gold_fact_order_item")


# COMMAND ----------

spark.sql(f"select count(*) from {catalog_name}.gold.gold_fact_order_item").show()

# COMMAND ----------

