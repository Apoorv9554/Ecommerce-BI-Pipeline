# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Dimension Data into Bronze Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType,DoubleType,LongType,ArrayType,MapType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brands

# COMMAND ----------

# Define schema for the data file
brand_schema = StructType([
  StructField("brand_code", StringType(), False),
  StructField("brand_name", StringType(), True),
  StructField("category_code", StringType(), True),
])

# COMMAND ----------

# DBTITLE 1,Cell 6
raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/brands.csv"
df = spark.read.option("header", True).option("delimiter",",").schema(brand_schema).csv(raw_data_path)
#add metadata columns
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at",F.current_timestamp())

#display(df)
display(df.limit(5))

# COMMAND ----------

#write to delta
df.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable("ecommerce.bronze.brand")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

category_schema = StructType([
  StructField("category_code", StringType(), False),
  StructField("category_name", StringType(), True),
])
#Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/category/category.csv"
df = spark.read.option("header", True).option("delimiter",",").schema(category_schema).csv(raw_data_path)

#add metadata columns
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at",F.current_timestamp())
#display(df)
display(df.limit(5))

# COMMAND ----------

#write to delta
df.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable("ecommerce.bronze.category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

from pyspark.sql.types import TimestampType

products_schema = StructType([
  StructField("product_id", StringType(), False),
  StructField("sku", StringType(), True),
  StructField("category_code", StringType(), True),
  StructField("brand_code", StringType(), True),
  StructField("color", StringType(), True),
  StructField("size", StringType(), True),
  StructField("material", StringType(), True),
  StructField("weight_grams", StringType(), True),
  StructField("length_cm", StringType(), True),
  StructField("height_cm", FloatType(), True),
  StructField("width_cm", FloatType(), True),
  StructField("rating_count", IntegerType(), True),
  StructField("file_name", StringType(), False),
  StructField("ingest_timestamp", StringType(), True)
])

#Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/products/products.csv"
df = spark.read \
    .option("header", True) \
    .option("delimiter", ",") \
    .schema(products_schema) \
    .csv(raw_data_path)

df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce.bronze.products")

# COMMAND ----------

spark.table("ecommerce.bronze.products").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/customers/customers.csv"

df = (
    spark.read
        .option("header", True)
        .option("delimiter", ",")
        .schema(customers_schema)
        .csv(raw_data_path)
)

# add metadata columns
df = (
    df.withColumn("_source_file", F.col("_metadata.file_path"))
      .withColumn("ingested_at", F.current_timestamp())
)

# overwrite Bronze table CLEANLY
df.write \
  .format("delta") \
  .mode("append") \
  .saveAsTable("ecommerce.bronze.customers")

# COMMAND ----------

#validation
spark.table("ecommerce.bronze.customers").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

date_schema = StructType([
    StructField("date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("week_of_year", IntegerType(), True)
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/date/date.csv"

df = (
    spark.read
        .option("header", True)
        .option("delimiter", ",")
        .schema(date_schema)
        .csv(raw_data_path)
)

df = (
    df.withColumn("_source_file", F.col("_metadata.file_path"))
      .withColumn("ingested_at", F.current_timestamp())
)

df.write \
  .format("delta") \
  .mode("append") \
  .saveAsTable("ecommerce.bronze.dates")

# COMMAND ----------

# MAGIC %md
# MAGIC