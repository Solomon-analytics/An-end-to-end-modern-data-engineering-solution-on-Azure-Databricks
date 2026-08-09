# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest channels csv file
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
# MAGIC # File: channels.csv
# MAGIC  - define channels source file and bronze table name using the environment variable
# MAGIC

# COMMAND ----------

channels_source_file = f"{landing_folder_path}/{v_batch_id}/channels.csv"
bronze_channels_name = f"{catalog_name}.{bronze_schema}.channels"


# COMMAND ----------

# MAGIC %md
# MAGIC # create channels schema
# MAGIC

# COMMAND ----------

channels_schema = StructType(fields=[
    StructField("channel_code", StringType()),
    StructField("channel_name", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the channels table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# read channels table with its defined schema
channels_df = (spark.read.format('csv').option('header', True).schema(channels_schema).option('mode', 'FAILFAST').load(channels_source_file))

# add ingestion metadata
channels_final_df = add_ingestion_metadata(channels_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    channels_final_df,
    bronze_channels_name,
    v_batch_id
)