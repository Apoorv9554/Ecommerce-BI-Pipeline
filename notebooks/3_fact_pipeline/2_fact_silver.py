# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: Data Cleansing and Transformation

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F


# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df = spark.table(f'{catalog_name}.bronze.order_items')
df.show(5)
                 

# COMMAND ----------

# Transformation: Drop any duplicate
df = df.dropDuplicates(["order_id", "item_seq"])

# Transformation: Convert 'Two' -> 2 and cast to Insert
df = df.withColumn("quantity",F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int"))

# Transformation: Remove any '$' or other symboles from unit_price, keep only numberic
df = df.withColumn("unit_price", F.regexp_replace("unit_price", r"[^0-9.\-]", "").cast("double"))

# Transformation: Remove '%' from discount_pct and cast to double
df = df.withColumn("discount_pct", F.regexp_replace("discount_pct", "[%]", "").cast("double"))

# Transformation: coupan code processing (convert to Lower)
df = df.withColumn("coupon_code",F.lower(F.trim(F.col("coupon_code"))))

# Transformation: channel processing
df = df.withColumn(
    "channel",
    F.when(F.lower(F.col("channel")) == "web", "Website")
     .when(F.lower(F.col("channel")) == "app", "Mobile")
     .otherwise(F.initcap(F.col("channel")))
)


# COMMAND ----------

#Transformations
#1.) convert dt (string -> date)
df = df.withColumn("dt", F.to_date("dt","yyyy-MM-dd"))

#2.) convert order_ts (string -> timestamp)
df = df.withColumn("order_ts", F.coalesce(
    F.to_timestamp("order_ts","yyyy-MM-dd HH:mm:ss"),
    F.to_timestamp("order_ts","dd-MM-yyyy HH:mm:ss") ))

#3.) Convert item_seq (string -> integer)
df = df.withColumn("item_seq", F.col("item_seq").cast("int"))

#4.) Convert tax_amount (string -> double)
df = df.withColumn("tax_amount", F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double"))

#Transformation : Add processed time
df = df.withColumn("processed_time", F.current_timestamp())

# COMMAND ----------

df_products = spark.table(f"{catalog_name}.silver.products")
df_customers = spark.table(f"{catalog_name}.silver.customers")
df_dates = spark.table(f"{catalog_name}.silver.dates")

# COMMAND ----------

df = df.join(
    df_products.select("product_id", "product_sk"),
    on="product_id",
    how="left"
)

# COMMAND ----------

df = df.join(
    df_customers.select("customer_id", "customer_sk"),
    on="customer_id",
    how="left"
)

# COMMAND ----------

df_dates = df_dates.withColumn(
    "date_sk",
    F.date_format("date", "yyyyMMdd").cast("int")
)

df = df.join(
    df_dates.select(
        F.col("date").alias("dt_join"),
        "date_sk"
    ),
    df.dt == F.col("dt_join"),
    "left"
).drop("dt_join")

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

print("Null product_sk:", df.filter(F.col("product_sk").isNull()).count())
print("Null customer_sk:", df.filter(F.col("customer_sk").isNull()).count())
print("Null date_sk:", df.filter(F.col("date_sk").isNull()).count())

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog_name}.silver.order_items")

# COMMAND ----------

