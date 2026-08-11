# Databricks notebook source
# MAGIC %md
# MAGIC # Transform exchange_rate bronze data
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
# MAGIC # Notebook: Adding dynamic workspace environment variable

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
# MAGIC # File: exchange_rates
# MAGIC  - define exchange_rates source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_exchange_rates_table = f"{catalog_name}.{bronze_schema}.exchange_rates"
silver_exchange_rates_table = f"{catalog_name}.{silver_schema}.exchange_rates"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

exchange_rates_df = spark.read.table(bronze_exchange_rates_table).filter(F.col("batch_id") == v_batch_id)


# COMMAND ----------

# MAGIC %md
# MAGIC - write to silver table

# COMMAND ----------

write_to_silver(
    exchange_rates_df,
    silver_exchange_rates_table,
    "t.currency = s.currency AND t.rate_month = s.rate_month", # perform a merge on currency and rate month, i case there is a chngang
    [
        "rate_to_gbp",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)