# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest subcategories csv file
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
# MAGIC # File: subcategories.csv
# MAGIC  - define subcategories source file and bronze table name using the environment variable
# MAGIC

# COMMAND ----------

subcategories_source_file = f"{landing_folder_path}/{v_batch_id}/subcategories.csv"
bronze_subcategories_name = f"{catalog_name}.{bronze_schema}.subcategories"


# COMMAND ----------

# MAGIC %md
# MAGIC # create subcategories schema
# MAGIC

# COMMAND ----------

subcategories_schema = StructType(fields=[
    StructField("subcategory_id", StringType()),
    StructField("subcategory_name", StringType()),
    StructField("category_name", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the subcategories table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the subcategories table with its defined schema
subcategories_df = spark.read.format('csv').option('header', True).schema(subcategories_schema).option('mode', 'FAILFAST').load(subcategories_source_file)

# add ingestion metadata
subcategories_final_df = add_ingestion_metadata(subcategories_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    subcategories_final_df,
    bronze_subcategories_name,
    v_batch_id
)