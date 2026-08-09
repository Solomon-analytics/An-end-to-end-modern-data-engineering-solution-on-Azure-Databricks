# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest cities csv file
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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add_ingestion_timestamp and ingest_to_bronze functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # File: cities.csv
# MAGIC  - define cities source file and bronze table name using the environment variable

# COMMAND ----------

cities_source_file = f"{landing_folder_path}/{v_batch_id}/cities.csv"
bronze_cities_name = f"{catalog_name}.{bronze_schema}.cities"


# COMMAND ----------

# MAGIC %md
# MAGIC # create cities schema
# MAGIC

# COMMAND ----------

cities_schema = StructType(fields=[
    StructField("city_id", StringType()),
    StructField("city_name", StringType()),
    StructField("region_id", StringType()),
    StructField("country", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the cities table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# read cities table with its defined schema
cities_df = spark.read.format('csv').option('header', True).schema(cities_schema).option('mode', 'FAILFAST').load(cities_source_file)

# add ingestion metadata
cities_final_df = add_ingestion_metadata(cities_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    cities_final_df,
    bronze_cities_name,
    v_batch_id
)