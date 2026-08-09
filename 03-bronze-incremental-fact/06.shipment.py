# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest shipment parquet file
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
# MAGIC # File: shipment.parquet
# MAGIC  - define shipment source file and bronze table name using the environment variable

# COMMAND ----------

shipment_source_file = f"{landing_folder_path}/{v_batch_id}/{shipment}"
bronze_shipment_name = f"{catalog_name}.{bronze_schema}.shipment"

# COMMAND ----------

# MAGIC %md
# MAGIC # create shipment schema
# MAGIC

# COMMAND ----------

shipment_schema = StructType(fields=[
    StructField('shipment_id', StringType()),
    StructField('order_id', StringType()),
    StructField('ship_date', TimestampType()),
    StructField('delivery_date', TimestampType()),
    StructField('carrier', StringType(), True),
    StructField('_loaded_at', TimestampType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the shipment table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the shipment table with its defined schema
shipment_df = spark.read.format('parquet').option('header', True).schema(shipment_schema).option('mode', 'FAILFAST').load(shipment_source_file)

# add ingestion metadata
shipment_final_df = add_ingestion_metadata(shipment_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    shipment_final_df,
    bronze_shipment_name,
    v_batch_id
)

