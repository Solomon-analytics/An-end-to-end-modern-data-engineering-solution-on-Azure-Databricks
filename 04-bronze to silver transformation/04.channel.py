# Databricks notebook source
# MAGIC %md
# MAGIC # Transform channels bronze data
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
# MAGIC # File: channel
# MAGIC  - define channels source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_channels_table = f"{catalog_name}.{bronze_schema}.channels"
silver_channels_table = f"{catalog_name}.{silver_schema}.channels"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

channels_df = spark.read.table(bronze_channels_table).filter(F.col("batch_id") == v_batch_id)


# COMMAND ----------

# MAGIC %md
# MAGIC - remove nulls from business keys
# MAGIC - remove duplicates

# COMMAND ----------

# remove nulls from business key: business key is channel_code
#channels_rem_nulls = channels_df.filter(F.col("channel_code").isNotNull())
channels_rem_nulls = remove_nulls(channels_df, ["channel_code"])

# drop duplicates using the bsuiess key colummn
channels_final_df = channels_rem_nulls.dropDuplicates(['channel_code'])


# COMMAND ----------

# MAGIC %md
# MAGIC - write to silver table

# COMMAND ----------

write_to_silver(
    channels_final_df,
    silver_channels_table,
    "t.channel_code = s.channel_code",
    [
        "channel_name",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)