# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest user_details csv file
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
# MAGIC # File: user_details.csv
# MAGIC  - define user_details source file and bronze table name using the environment variable

# COMMAND ----------

user_details_source_file = f"{landing_folder_path}/{v_batch_id}/user_details.csv"
bronze_user_details_name = f"{catalog_name}.{bronze_schema}.user_details"

# COMMAND ----------

# MAGIC %md
# MAGIC # create user_details schema
# MAGIC

# COMMAND ----------

user_details_schema = StructType(fields=[
    StructField('user_email', StringType()),
    StructField('full_name', StringType()),
    StructField('region_id', StringType()),
    StructField('job_title', StringType()),
    StructField('is_active', StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the user_details table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the user_details table with its defined schema
user_details_df = spark.read.format('csv').option('header', True).schema(user_details_schema).option('mode', 'FAILFAST').load(user_details_source_file)

# add ingestion metadata
user_details_final_df = add_ingestion_metadata(user_details_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    user_details_final_df,
    bronze_user_details_name,
    v_batch_id
)