# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: Data Cleaning and Transformation Dimension Tables

# COMMAND ----------

#STEP 1: Read Bronze Table
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_bronze = spark.table("ecommerce.bronze.brand")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brand

# COMMAND ----------

#STEP 2: Clean & Standardize Columns
#✔ Clean brand_code (remove special chars, uppercase)
#✔ Clean brand_name (trim spaces)
#✔ Standardize category_code
df_clean = (
    df_bronze
    .withColumn(
        "brand_code",
        F.upper(F.regexp_replace(F.col("brand_code"), "[^A-Z0-9]", ""))
    )
    .withColumn(
        "brand_name",
        F.initcap(F.trim(F.col("brand_name")))
    )
    .withColumn(
        "category_code",
        F.upper(F.trim(F.col("category_code")))
    )
)


# COMMAND ----------

#STEP 3: Remove Duplicates
window_spec = Window.partitionBy("brand_code").orderBy(F.col("ingested_at").desc())

df_dedup = (
    df_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

#STEP 4: Add Surrogate Key (VERY IMPORTANT)
df_silver = df_dedup.withColumn("brand_sk", F.abs(F.xxhash64("brand_code")))

# COMMAND ----------

#STEP 5: Select Final Silver Columns
df_silver_final = df_silver.select(
    "brand_sk",
    "brand_code",
    "brand_name",
    "category_code",
    "_source_file",
    "ingested_at"
)

# COMMAND ----------

#BRAND VALIDATION
print("Null brand_code:", df_silver_final.filter(F.col("brand_code").isNull()).count())
print("Null category_code:", df_silver_final.filter(F.col("category_code").isNull()).count())

# COMMAND ----------

#STEP 6: Write Silver Table
df_silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("ecommerce.silver.brand")

spark.sql("OPTIMIZE ecommerce.silver.brand")

# COMMAND ----------

df_silver_final.select("category_code").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category
# MAGIC

# COMMAND ----------

# STEP 1: Read Bronze Table
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_bronze = spark.table("ecommerce.bronze.category")

# COMMAND ----------

#STEP 2: Basic Cleaning
df_clean = (
    df_bronze
    .withColumn("category_code", F.upper(F.trim("category_code")))
    .withColumn("category_name", F.initcap(F.trim("category_name")))
)

# COMMAND ----------

df_clean = df_clean.withColumn(
    "category_display",
    F.when(F.col("category_code") == "BKS", "Books")
     .when(F.col("category_code") == "GRCY", "Grocery")
     .when(F.col("category_code") == "TOY", "Toys")
     .when(F.col("category_code") == "CE", "Consumer Electronics")
     .when(F.col("category_code") == "APP", "Apparel")
     .when(F.col("category_code") == "HNK", "Home And Kitchen")
     .when(F.col("category_code") == "BPC", "Beauty And Personal Care")
     .when(F.col("category_code") == "SPT", "Sports")
     .otherwise("Unknown")
)

# COMMAND ----------

window_spec = Window.partitionBy("category_code").orderBy(
    F.col("ingested_at").desc()
)

df_dedup = (
    df_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

#STEP 5: Add Surrogate Key
df_silver = df_dedup.withColumn(
    "category_sk",
    F.abs(F.xxhash64("category_code"))
)

# COMMAND ----------

#CATEGORY VALIDATION
print("Null category_code:", df_silver.filter(F.col("category_code").isNull()).count())

# COMMAND ----------

#STEP 6: Write Silver Table
df_silver.select(
    "category_sk",
    "category_code",
    "category_name",
    "category_display",
    "_source_file",
    "ingested_at"
).write.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("ecommerce.silver.category")

spark.sql("OPTIMIZE ecommerce.silver.category")

# COMMAND ----------

spark.table("ecommerce.silver.category") \
     .select("category_code") \
     .distinct() \
     .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_bronze = spark.table("ecommerce.bronze.products")

# COMMAND ----------

#get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)
#print the result
print(f"Row count: {row_count}, Column count: {column_count}")


# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

#STEP 2: Basic String Cleaning
df_clean = (
    df_bronze
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("sku", F.upper(F.trim("sku")))
    .withColumn("brand_code", F.upper(F.regexp_replace("brand_code", "[^A-Z0-9]", "")))
    .withColumn("category_code", F.upper(F.trim("category_code")))
    .withColumn("color", F.initcap(F.trim("color")))
    .withColumn("material", F.initcap(F.trim("material")))
    .withColumn("size", F.upper(F.trim("size")))
)

# COMMAND ----------

#STEP 3: Fix Numeric Columns (CRITICAL)
df_numeric = (
    df_clean
    .withColumn(
        "weight_grams",
        F.regexp_replace("weight_grams", "[^0-9.]", "").cast("double")
    )
    .withColumn(
        "length_cm",
        F.regexp_replace("length_cm", ",", ".").cast("double")
    )
    .withColumn("height_cm", F.col("height_cm").cast("double"))
    .withColumn("width_cm", F.col("width_cm").cast("double"))
    .withColumn(
        "rating_count",
        F.when(F.col("rating_count") < 0, 0).otherwise(F.col("rating_count"))
    )
)


# COMMAND ----------

# DBTITLE 1,Cell 26
#STEP 4: Remove Duplicates
window_spec = Window.partitionBy("product_id").orderBy(F.col("ingested_at").desc())

df_dedup = (
    df_numeric
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

material_map = {
    "COTON": "Cotton",
    "COTTON": "Cotton",
    "RUBER": "Rubber",
    "RUBBER": "Rubber",
    "STEEL": "Steel",
    "WOOD": "Wood",
    "PLASTIC": "Plastic",
    "METAL": "Metal",
    "LEATHER": "Leather"
}

# COMMAND ----------

# DBTITLE 1,Cell 27.5
#STEP 5: Add Surrogate Key and Create Silver DataFrame
df_silver = df_dedup.withColumn(
    "product_sk",
    F.abs(F.xxhash64("product_id"))
)

# COMMAND ----------

#STEP 6: Add Surrogate Key
df_final = (
    df_silver
    .withColumn("material", F.initcap(F.trim("material")))
    .withColumn(
        "brand_code",
        F.upper(F.regexp_replace(F.trim("brand_code"), "[^A-Z0-9]", ""))
    )
    .replace(to_replace=material_map, subset=["material"])
)

# COMMAND ----------

#STEP 8: Fix Spellin Mistakes
material_map = {
    "COTON": "Cotton",
    "COTTON": "Cotton",
    "RUBER": "Rubber",
    "RUBBER": "Rubber",
    "STEEL": "Steel",
    "WOOD": "Wood",
    "PLASTIC": "Plastic",
    "METAL": "Metal",
    "LEATHER": "Leather"
}

# COMMAND ----------

#PRODUCTS VALIDATION
print("Null product_id:", df_final.filter(F.col("product_id").isNull()).count())
print("Null brand_code:", df_final.filter(F.col("brand_code").isNull()).count())
print("Null category_code:", df_final.filter(F.col("category_code").isNull()).count())

# COMMAND ----------

#FOREIGN KEY VALIDATION - PRODUCTS vs CATEGORY VALIDATION
df_category = spark.table("ecommerce.silver.category")

invalid_category = df_final.join(
    df_category,
    "category_code",
    "left_anti"
)

print("Invalid category_code rows:", invalid_category.count())

# COMMAND ----------

# PRODUCTS vs BRAND VALIDATION
df_brand = spark.table("ecommerce.silver.brand")

invalid_brand = df_final.join(
    df_brand,
    "brand_code",
    "left_anti"
)

print("Invalid brand_code rows:", invalid_brand.count())

# COMMAND ----------

# DBTITLE 1,Cell 30
#STEP 7: Write Silver Table
df_final.write \
  .mode("overwrite") \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .saveAsTable("ecommerce.silver.products")

spark.sql("OPTIMIZE ecommerce.silver.products")


# COMMAND ----------

#Check row count
spark.table("ecommerce.silver.products").count()

# COMMAND ----------

#Check category codes
spark.table("ecommerce.silver.products") \
     .select("material") \
     .distinct() \
     .show()

# COMMAND ----------

df_final.select("category_code").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer

# COMMAND ----------

df_bronze = spark.table("ecommerce.bronze.customers")

# 1.Trim & uppercase codes
# 2. Normalize country names
# 3. Validate phone length
# 4. Handle NULL phones
# 5. Deduplicate customers
# 6. Add customer_sk
window_spec = Window.partitionBy("customer_id").orderBy(F.col("ingested_at").desc())
df_silver = (
    df_bronze
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn("country_code", F.upper(F.trim("country_code")))
    .withColumn("country", F.initcap(F.trim("country")))
    .withColumn("state", F.upper(F.trim("state")))
    .withColumn(
        "phone",
        F.regexp_replace("phone", "\\.0$", "")
    )
    .filter(F.col("customer_id").isNotNull())
    .withColumn("customer_sk", F.abs(F.xxhash64("customer_id")))
)





# COMMAND ----------

#Customer Validation
print("Null customer_id:", df_silver.filter(F.col("customer_id").isNull()).count())

# COMMAND ----------

df_silver.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("ecommerce.silver.customers")

spark.sql("OPTIMIZE ecommerce.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date

# COMMAND ----------

# DBTITLE 1,Cell 37
from pyspark.sql import functions as F

df_bronze = spark.table("ecommerce.bronze.dates")

df_silver = (
    df_bronze
    .withColumn("date", F.to_date("date", "dd-MM-yyyy"))
    .filter(F.col("date").isNotNull())
    .withColumn("date_sk",F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .dropDuplicates(["date"])
)

# COMMAND ----------

#Date Validation
print("Null date:", df_silver.filter(F.col("date").isNull()).count())

# COMMAND ----------

df_silver.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("ecommerce.silver.dates")

spark.sql("OPTIMIZE ecommerce.silver.dates")

# COMMAND ----------

spark.table("ecommerce.silver.dates") \
     .select("date", "day_name", "week_of_year", "quarter") \
     .orderBy("date") \
     .show(10, truncate=False)

# COMMAND ----------

row_count = df_silver.count()
print ("Rows ", row_count )

# COMMAND ----------

