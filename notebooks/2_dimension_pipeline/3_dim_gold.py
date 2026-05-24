# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold: Building BI Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

df_products = spark.table(f"{catalog_name}.silver.products")
df_brands = spark.table(f"{catalog_name}.silver.brand")
df_category = spark.table(f"{catalog_name}.silver.category")


# COMMAND ----------

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

# COMMAND ----------

display(spark.sql("select * from v_products limit 5"))
display(spark.sql("select * from v_brands limit 5"))
display(spark.sql("select * from v_category limit 5"))

# COMMAND ----------

#Make sure we're on the right catalog
spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ecommerce.gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brands_categories AS (
# MAGIC     SELECT DISTINCT 
# MAGIC     b.brand_sk,
# MAGIC     b.brand_name,
# MAGIC     b.brand_code,
# MAGIC     c.category_sk,
# MAGIC     c.category_name,
# MAGIC     c.category_code
# MAGIC     FROM v_brands b
# MAGIC     INNER JOIN v_category c
# MAGIC         ON b.category_code = c.category_code
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     p.product_sk,
# MAGIC     bc.brand_sk,
# MAGIC     bc.category_sk,
# MAGIC     p.product_id, 
# MAGIC     p.sku,
# MAGIC     p.category_code, 
# MAGIC     COALESCE(bc.category_name,'Not Available') AS category_name, 
# MAGIC     p.brand_code, 
# MAGIC     COALESCE(bc.brand_name,'Not Available') AS brand_name, 
# MAGIC     p.color, 
# MAGIC     p.size, 
# MAGIC     p.material, 
# MAGIC     p.weight_grams, 
# MAGIC     p.length_cm, 
# MAGIC     p.height_cm, 
# MAGIC     p.width_cm,
# MAGIC     p.rating_count,
# MAGIC     p.file_name,
# MAGIC     p.ingest_timestamp
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brands_categories bc
# MAGIC
# MAGIC ON p.brand_code = bc.brand_code
# MAGIC AND p.category_code = bc.category_code
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

#Indian states
indian_region = {
    "MH" : "West",
    "GJ" : "West",
    "RJ" : "West",
    "KA" : "South",
    "TN" : "South",
    "TS" : "South",
    "AP" : "South",
    "KL" : "South",
    "UP" : "North",
    "DL" : "North"
}

 
#Australia States
australia_region = {
    "QLD":"NorthEast", "NSW":"East", "WA":"East", "VIC":"SouthEast"}
 
#United Kingdom states
uk_region = {
    "ENG:":"England", "WLS":"Wales","NIR": "NorthernIreland", "SCT":"Scotland"
}
 
#United States state
us_region = {
    "MA":"NorthernEast", "FL":"South", "NJ":"NorthEast", "CA":"West","NY":"NorthEast","TX":"South"
}
 
# UAE staes
uae_region = {
    "AUH":"Abu Dhabi","DU":"Dubai","SHJ":"Sharjah"
}
 
#Singapore states
singapore_region = {
    "SG":"Singapore"
}
 
# Canada staes
canada_region = {
    "QC":"East","ON":"East","AB":"West","BC":"West","NS":"East","IL":"Other"
    }
 
   
# Combine into a master dictionary
country_state_map = {
    "India": indian_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "UAE": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}

# COMMAND ----------

country_state_map

# COMMAND ----------

# Flatten country_state_map into a list of rows
rows = []
for country,states in country_state_map.items():
    for state_code,region in states.items():
        rows.append(Row(country=country,state=state_code,region=region))
rows[:100]


# COMMAND ----------

# 2. Create mapping DataFrame
df_region_mapping = spark.createDataFrame(rows)
#Optional: Show mapping
df_region_mapping.show(truncate = False)

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.customers')
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on = ['country','state'], how = 'left')
df_gold = df_gold.fillna({'region':'Other'})
display(df_gold.limit(5))


# COMMAND ----------

# Write to Delta Lake
df_gold.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.gold.gld_dim_customers')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data/Calender
# MAGIC

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.dates')
display(df_silver.limit(5))


# COMMAND ----------

df_gold = df_silver.withColumn("date_sk", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

#Add month name(e.g., 'January','February',etc.)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"),"MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn("is_weekend", F.when(F.col("day_name").isin("Saturday","Sunday"),1).otherwise(0))

display(df_gold.limit(5))


# COMMAND ----------

desired_columns_order = ["date_sk","date","year","day_name","quarter","week_of_year","is_weekend","ingested_at","_source_file"]
df_gold = df_gold.select(desired_columns_order)
display(df_gold.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 23

# Write to Delta Lake
df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'{catalog_name}.gold.gld_dim_date')