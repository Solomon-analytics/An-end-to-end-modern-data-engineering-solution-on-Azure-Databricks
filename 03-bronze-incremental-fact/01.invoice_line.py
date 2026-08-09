# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest invoice_line parquet file
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

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add_ingestion_timestamp and ingest_to_bronze functions

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: invoice_line.parquet
# MAGIC  - define invoice_line source file and bronze table name using the environment variable

# COMMAND ----------

invoice_lines_source_file = f"{landing_folder_path}/{v_batch_id}/{invoice_line_path}"
bronze_invoice_lines_name = f"{catalog_name}.{bronze_schema}.invoice_lines"

# COMMAND ----------

# MAGIC %md
# MAGIC # create invoice_lines schema
# MAGIC

# COMMAND ----------

invoice_lines_schema = StructType(fields=[
    StructField("invoice_no", StringType()),
    StructField("line_no", LongType()),
    StructField("line_id", StringType()),
    StructField("amount", DoubleType()),
    StructField('_loaded_at', TimestampType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the invoice_lines table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the invoice_lines table with its defined schema
invoice_lines_df = spark.read.format('parquet').option('header', True).schema(invoice_lines_schema).option('mode', 'FAILFAST').load(invoice_lines_source_file)

# add ingestion metadata
invoice_lines_final_df = add_ingestion_metadata(invoice_lines_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    invoice_lines_final_df,
    bronze_invoice_lines_name,
    v_batch_id
)
