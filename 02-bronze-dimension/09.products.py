# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest products csv file
# MAGIC - considering file is saved in a batch folder, set a parameter, batch_id
# MAGIC - Read the files using spark dataframe reader API
# MAGIC - Add metadata Columns
# MAGIC     - Source file 
# MAGIC     - ingestion timestamp
# MAGIC - Write all files to its bronze delta table
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md
# MAGIC # set parameter and variable: batch_id
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic workspace environment variable
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add_ingestion_timestamp and ingest_to_bronze functions

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: products.csv
# MAGIC  - define products source file and bronze table name using the environment variable
# MAGIC

# COMMAND ----------

products_source_file = f"{landing_folder_path}/{v_batch_id}/products.csv"
bronze_products_name = f"{catalog_name}.{bronze_schema}.products"


# COMMAND ----------

# MAGIC %md
# MAGIC # create products schema
# MAGIC

# COMMAND ----------

products_schema = StructType(fields=[
    StructField('sku', StringType()),
    StructField('product_name', StringType()),
    StructField('brand', StringType()),
    StructField('subcategory_id', StringType()),
    StructField('price', FloatType()),
    StructField('cost', FloatType()),
    StructField('supplier', StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the products table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the products table with its defined schema
products_df = spark.read.format('csv').option('header', True).schema(products_schema).option('mode', 'FAILFAST').load(products_source_file)

# Add ingestion metadata
products_final_df = add_ingestion_metadata(products_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    products_final_df,
    bronze_products_name,
    v_batch_id
)