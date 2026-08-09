# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest address csv file
# MAGIC - considering file is saved in a batch folder, set a parameter, batch_id
# MAGIC - Read the files using spark dataframe reader API
# MAGIC - Add metadata Columns
# MAGIC     - Source file 
# MAGIC     - ingestion timestamp
# MAGIC - Write all files to its bronze delta table

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
# MAGIC

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
# MAGIC # File: Address.csv
# MAGIC  - define address source file and bronze table name using the environment variable

# COMMAND ----------

address_source_file = f"{landing_folder_path}/{v_batch_id}/Address.csv"
bronze_address_name = f"{catalog_name}.{bronze_schema}.address"

# COMMAND ----------

# MAGIC %md
# MAGIC # create address schema

# COMMAND ----------

address_schema = StructType(fields=[
    StructField("address_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("street", StringType()),
    StructField("city_id", StringType()),
    StructField("postcode", StringType()),
    StructField("address_type", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the address table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table

# COMMAND ----------

# read the address table with the defined schema
address_df = spark.read.format("csv").option("header", True).schema(address_schema).option('mode', 'FAILFAST').load(address_source_file)

# Add ingestion metadata
address_final_df = add_ingestion_metadata(address_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    input_df = address_final_df,
    target_table = bronze_address_name,
    batch_id = v_batch_id
)