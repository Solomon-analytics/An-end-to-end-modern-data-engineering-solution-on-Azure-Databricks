# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest campaign_skus csv file
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
# MAGIC # File: campaign_skus.csv
# MAGIC  - define campaign_skus source file and bronze table name using the environment variable
# MAGIC

# COMMAND ----------

campaign_sku_source_file = f"{landing_folder_path}/{v_batch_id}/campaign_skus.csv"
bronze_campaign_sku_name = f"{catalog_name}.{bronze_schema}.campaign_sku"


# COMMAND ----------

# MAGIC %md
# MAGIC # create campaign_sku schema
# MAGIC

# COMMAND ----------

campaign_sku_schema = StructType(fields=[
    StructField("campaign_id", StringType()),
    StructField("sku", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the campaign_sku table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# read campaign sku table with its defined schema
campaign_sku_df = spark.read.format("csv").option("header", "true").schema(campaign_sku_schema).option("mode", "FAILFAST").load(campaign_sku_source_file)

# add ingestion metadata
campaign_sku_final_df = add_ingestion_metadata(campaign_sku_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    campaign_sku_final_df,
    bronze_campaign_sku_name,
    v_batch_id
)