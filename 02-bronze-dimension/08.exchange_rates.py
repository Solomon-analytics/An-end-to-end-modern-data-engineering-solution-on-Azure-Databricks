# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest exchange_rates csv file
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
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic workspace environment variable
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add_ingestion_timestamp and ingest_to_bronze functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: exchange_rates.csv
# MAGIC  - define exchange_rates source file and bronze table name using the environment variable
# MAGIC
# MAGIC

# COMMAND ----------

exchange_rates_source_file = f"{landing_folder_path}/{v_batch_id}/exchange_rates.csv"
bronze_exchange_rates_name = f"{catalog_name}.{bronze_schema}.exchange_rates"

# COMMAND ----------

# MAGIC %md
# MAGIC # create exchange_rates schema
# MAGIC

# COMMAND ----------

exchange_rates_schema = StructType(fields=[
    StructField("currency", StringType()),
    StructField("rate_month", DateType()),
    StructField("rate_to_gbp", FloatType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the exchange_rates table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the exchange_rates table with its defined schema
exchange_rates_df = spark.read.format("csv").option('header', True).schema(exchange_rates_schema).option('mode', 'FAILFAST').load(exchange_rates_source_file)

# Add ingesttion metadata
exchange_rates_final_df = add_ingestion_metadata(exchange_rates_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    exchange_rates_final_df,
    bronze_exchange_rates_name,
    v_batch_id
)