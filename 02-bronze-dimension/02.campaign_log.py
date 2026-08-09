# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest campaign_log csv file
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
# MAGIC # File: CAMPAIGN_LOG.csv
# MAGIC  - define CAMPAIGN_LOG source file and bronze table name using the environment variable

# COMMAND ----------

campaign_log_source_file = f"{landing_folder_path}/{v_batch_id}/CAMPAIGN_LOG.csv"
bronze_campaign_log_name = f"{catalog_name}.{bronze_schema}.campaign_log"


# COMMAND ----------

# MAGIC %md
# MAGIC # create campaign_log schema

# COMMAND ----------

campaign_log_schema = StructType(fields=[
    StructField("CAMPAIGN_ID", StringType()),
    StructField("CAMPAIGN_NAME", StringType()),
    StructField("CHANNEL", StringType()),
    StructField("BUDGET", IntegerType()),
    StructField("START_DT", DateType()),
    StructField("End_DT", DateType()),
    StructField("LOG_DATE", DateType()),
    StructField("IMPRESSIONS", IntegerType()),
    StructField("CLICKS", IntegerType()),
    StructField("SPEND", FloatType())
]
)
                                 

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the campaign_log table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table

# COMMAND ----------

# Read campaign log table with its defined schema
campaign_log_df = spark.read.format('csv').option("header", True).schema(campaign_log_schema).option('mode', 'FAILFAST').load(campaign_log_source_file)

# add ingestion_metadata
campaign_log_final_df = add_ingestion_metadata(campaign_log_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    input_df = campaign_log_final_df,
    target_table = bronze_campaign_log_name,
    batch_id = v_batch_id
)