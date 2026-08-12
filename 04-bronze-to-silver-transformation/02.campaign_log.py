# Databricks notebook source
# MAGIC %md
# MAGIC # Transform campaign_log bronze data
# MAGIC  - Read file using spark dataframe reader API
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names
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
# MAGIC # File: campaign_log
# MAGIC  - define campaign_log source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_campaign_log_table = f"{catalog_name}.{bronze_schema}.campaign_log"
silver_campaign_log_table = f"{catalog_name}.{silver_schema}.campaign_log"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

campaign_log = spark.read.table(bronze_campaign_log_table).filter(F.col("batch_id") == v_batch_id)


# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

campaign_log_seleted_df = campaign_log.select(
    F.col("CAMPAIGN_ID").alias("campaign_id"),
    F.col("CAMPAIGN_NAME").alias("campaign_name"),
    F.col("CHANNEL").alias("campaign_channel"),
    F.col("BUDGET").alias("campaign_budget"),
    F.col("START_DT").alias("campaign_start_date"),
    F.col("End_DT").alias("campaign_end_date"),
    F.col("LOG_DATE").alias("campaign_log_date"),
    F.col("IMPRESSIONS").alias("campaign_impressions"),
    F.col("CLICKS").alias("campaign_clicks"),
    F.col("SPEND").alias("campaign_spend"),
    F.col("ingestion_timestamp").alias("ingestion_timestamp"),
    F.col("source_file").alias("source_file"),
    F.col("batch_id").alias("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# DBTITLE 1,Cell 17
# setting up logging for auditing purposes
before_transformation = campaign_log_seleted_df.count()

# Remove white spaces from string columns
campaign_log_seleted_df = trim_whitespaces(campaign_log_seleted_df)

# remove nulls from business key: there is no unique key in this table. the following column unqiuely identifies a each record
#display(campaign_log_seleted_df.groupBy(["campaign_id", "campaign_name", "campaign_start_date", "campaign_channel", "campaign_log_date"]).agg(F.count("*")).filter(F.col( "count(1)") > 1))

campaign_log_not_null = remove_nulls(campaign_log_seleted_df, ["campaign_id", "campaign_name", "campaign_start_date", "campaign_channel", "campaign_log_date"])

print(f"before dropping nulls: {before_transformation}, after dropping nulls: {campaign_log_not_null.count()}")

# remove duplicates
campaign_log_rem_dup = campaign_log_not_null.dropDuplicates(["campaign_id", "campaign_name", "campaign_start_date", "campaign_channel", "campaign_log_date"])
print(f"before dropping duplicates: {campaign_log_not_null.count()}, after dropping duplicates: {campaign_log_rem_dup.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    campaign_log_rem_dup,
    silver_campaign_log_table,
    "t.campaign_id = s.campaign_id AND t.campaign_name = s.campaign_name AND t.campaign_start_date = s.campaign_start_date AND t.campaign_channel = s.campaign_channel AND t.campaign_log_date = s.campaign_log_date",
    [
        "campaign_budget",
        "campaign_end_date",
        "campaign_impressions",
        "campaign_clicks",
        "campaign_spend",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)