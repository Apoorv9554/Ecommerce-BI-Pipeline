# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ecommerce.bronze;
# MAGIC create schema if not exists ecommerce.silver;
# MAGIC create schema if not exists ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases from ecommerce;

# COMMAND ----------

# %sql
# Drop catalog if exists ecommerce CASCADE;

# COMMAND ----------

