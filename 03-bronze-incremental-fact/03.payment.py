# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest payment parquet file
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
# MAGIC # File: payment.parquet
# MAGIC  - define payment source file and bronze table name using the environment variable
# MAGIC

# COMMAND ----------

payment_source_file = f"{landing_folder_path}/{v_batch_id}/{payment}"
bronze_payment_name = f"{catalog_name}.{bronze_schema}.payment"

# COMMAND ----------

# MAGIC %md
# MAGIC # create payment_log schema
# MAGIC

# COMMAND ----------

payment_schema = StructType(fields=[
    StructField('payment_id', StringType()),
    StructField('invoice_no', StringType()),
    StructField('pay_date', TimestampType()),
    StructField('amount', DoubleType()),
    StructField('method', StringType()),
    StructField('_loaded_at', TimestampType())
]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the payment table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the payment table with its defined schema
payment_df = spark.read.format('parquet').option('header', True).schema(payment_schema).option('mode', 'FAILFAST').load(payment_source_file)

# add ingestion metadata
payment_final_df = add_ingestion_metadata(payment_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    payment_final_df,
    bronze_payment_name,
    v_batch_id
)