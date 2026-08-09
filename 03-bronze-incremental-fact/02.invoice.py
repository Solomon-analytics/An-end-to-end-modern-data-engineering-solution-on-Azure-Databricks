# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest invoice csv file
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

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic workspace environment variable
# MAGIC
# MAGIC
# MAGIC
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
# MAGIC # File: invoice.parquet
# MAGIC  - define invoice source file and bronze table name using the environment variable
# MAGIC
# MAGIC

# COMMAND ----------


invoice_source_file = f"{landing_folder_path}/{v_batch_id}/{invoice}"
bronze_invoice_name = f"{catalog_name}.{bronze_schema}.invoice"

# COMMAND ----------

# MAGIC %md
# MAGIC # create invoice schema
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # create invoice schema
# MAGIC

# COMMAND ----------

invoice_schema = StructType(fields=[
    StructField('invoice_no', StringType()),
    StructField('order_id', StringType()),
    StructField('customer_id', LongType()),
    StructField('invoice_date', TimestampType(), True),
    StructField('currency', StringType()),
    StructField('invoice_total', DoubleType()),
    StructField('_loaded_at', TimestampType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the invoice table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the invoice table with its defined schema
invoice_df = spark.read.format('parquet').option('header', True).schema(invoice_schema).option('mode', 'FAILFAST').load(invoice_source_file)

# add ingestion8 metadata
invoice_final_df = add_ingestion_metadata(invoice_df)

# write to bronze table
write_to_bronze(
    invoice_final_df,
    bronze_invoice_name,
    v_batch_id
)