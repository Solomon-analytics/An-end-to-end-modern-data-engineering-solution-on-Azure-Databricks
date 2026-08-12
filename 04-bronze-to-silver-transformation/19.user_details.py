# Databricks notebook source
# MAGIC %md
# MAGIC # Transform user_details bronze data
# MAGIC  - Read file using spark dataframe reader API
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates
# MAGIC  - Transform values in string columns to title_case
# MAGIC  - Apply business transformation rules
# MAGIC  - Write transformed data to silver table
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # set parameter and variable: batch_id
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC > # Notebook: Adding dynamic workspace environment variable

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add metadata and ingest_to_silver functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: user_details
# MAGIC  - define user_details source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_user_details_table = f"{catalog_name}.{bronze_schema}.user_details"
silver_user_details_table = f"{catalog_name}.{silver_schema}.user_details"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

user_details_df = spark.read.table(bronze_user_details_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

write_to_silver(
    user_details_df,
    silver_user_details_table,
    "t.full_name = s.full_name AND t.region_id = s.region_id",
    [
        "user_email",
        "job_title",
        "is_active",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)