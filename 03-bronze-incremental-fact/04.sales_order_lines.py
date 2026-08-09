# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest sales order lines parquet file
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

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add_ingestion_timestamp and ingest_to_bronze functions

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: sales_order_lines.parquet
# MAGIC  - define sales_order_lines source file and bronze table name using the environment variable

# COMMAND ----------

sales_order_lines_source_file = f"{landing_folder_path}/{v_batch_id}/{sales_order_lines}"
bronze_sales_order_lines_name = f"{catalog_name}.{bronze_schema}.sales_order_lines"


# COMMAND ----------

# MAGIC %md
# MAGIC # create sales_order_lines schema
# MAGIC

# COMMAND ----------

sales_orer_lines_schema = StructType(fields=[
    StructField('line_id', StringType()),
    StructField('order_id', StringType()),
    StructField('sku', StringType()),
    StructField('Quantity', LongType()),
    StructField('unit_price', DoubleType()),
    StructField('discount_pct', DoubleType()),
    StructField('line_total', DoubleType()),
    StructField('_loaded_at', TimestampType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the sales_order_lines table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the sales_order_lines table with its defined schema
sales_order_lines_df = spark.read.format('parquet').option('header', True).schema(sales_orer_lines_schema).option('mode', 'FAILFAST').load(sales_order_lines_source_file)

# add ingestion metadata
sales_order_lines_final_df = add_ingestion_metadata(sales_order_lines_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    sales_order_lines_final_df,
    bronze_sales_order_lines_name,
    v_batch_id
)