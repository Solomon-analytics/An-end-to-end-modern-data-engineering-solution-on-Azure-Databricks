# Databricks notebook source
# MAGIC %md
# MAGIC # Transform products bronze data
# MAGIC  - Read file using spark dataframe reader API
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates
# MAGIC  - Transform values in string columns to title_case
# MAGIC  - Apply business transformation rules
# MAGIC  - Write transformed data to silver table
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # set parameter and variable: batch_id
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC > # Notebook: Adding dynamic workspace environment variable

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add metadata and ingest_to_silver functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: products
# MAGIC  - define products source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_products_table = f"{catalog_name}.{bronze_schema}.products"
silver_products_table = f"{catalog_name}.{silver_schema}.products"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

products_df = spark.read.table(bronze_products_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

products_rename_df = products_df.withColumnsRenamed({
    "sku":"product_sku",
    "subcategory_id":"sub_category_id",
    "price":"product_price",
    "cost":"product_cost",
    "supplier":"product_supplier",
    "brand":"product_brand"
})

# COMMAND ----------

# MAGIC  %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup loggins for audit purpose
row_count = products_rename_df.count()

# remove whitespaces from string columns
products_rem_space = trim_whitespaces(products_rename_df)

# remove nulls from business keys --> 1200 row count --> product_sku uniquely represent each record
#display(products_rem_space.groupBy("product_sku").agg(F.count("*").alias("count")).filter(F.col("count")>1))
products_rem_nulls = remove_nulls(products_rem_space, ["product_sku"])

# drop duplicates
products_final_df = products_rem_nulls.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    products_final_df,
    silver_products_table,
    "t.product_sku = s.product_sku",
    [
        "product_name",
        "product_brand",
        "sub_category_id",
        "product_price",
        "product_cost",
        "product_supplier",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)