# Databricks notebook source
# MAGIC %md
# MAGIC # Transform campaign_sku bronze data
# MAGIC  - Read file using spark dataframe reader API
# MAGIC c
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates
# MAGIC  - Transform values in string columns to title_case
# MAGIC  - Write transformed data to silver table
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add metadata and ingest_to_silver functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: campaign_sku
# MAGIC  - define campaign_sku source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_campaign_sku_table = f"{catalog_name}.{bronze_schema}.campaign_sku"
silver_campaign_sku_table = f"{catalog_name}.{silver_schema}.campaign_sku"


# COMMAND ----------

# MAGIC %md
# MAGIC - Read file using spark dataframe reader API

# COMMAND ----------

campaign_sku_df = spark.read.table(bronze_campaign_sku_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

campaign_sku_selected_df = campaign_sku_df.select(
    F.col("campaign_id").alias("campaign_id"),
    F.col("sku").alias("campaign_sku"),
    F.col("ingestion_timestamp").alias("ingestion_timestamp"),
    F.col("source_file").alias("source_file"),
    F.col("batch_id").alias("batch_id")

)

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - remove duplicates

# COMMAND ----------

# setup loggins for audit purpose
row_count = campaign_sku_selected_df.count()


# remove white space from  string columns
campaign_sku_rem_df = trim_whitespaces(campaign_sku_selected_df)

# remove nulls: understanding the grain
#display(campaign_sku_rem_df.groupBy(["campaign_id","campaign_sku"]).count().filter(F.col("count")>1))# returns one instance
#display(campaign_sku_rem_df.filter(F.col("campaign_id")==24))# there are two instance with campaign sku as null, related to campaign id ==24
#display(campaign_sku_rem_df.filter(F.col("campaign_sku").isNull()))# there are four instance where campaign_sku is null, all tied to campaign_id 10, 15, and 24(*2)
# individually dropping nulls from campaign_id and campaign_sku
campaign_sku_drop1 = campaign_sku_rem_df.dropna(subset=["campaign_id"])
campaign_sku_drop2 = campaign_sku_drop1.dropna(subset=["campaign_sku"])

print(f"before dropping nulls: {row_count} | after dropping nulls {campaign_sku_drop2.count()}")

# dropping duplicates combining both columns
campaign_sku_drop_dup = campaign_sku_drop2.dropDuplicates(subset=["campaign_id","campaign_sku"])
print(f"before dropping duplicates: {row_count} | after dropping duplicates: {campaign_sku_drop_dup.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    campaign_sku_drop_dup,
    silver_campaign_sku_table,
    "t.campaign_id = s.campaign_id and t.campaign_sku = s.campaign_sku",
    [
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)