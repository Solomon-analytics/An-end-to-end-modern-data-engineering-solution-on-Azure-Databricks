# Databricks notebook source
# MAGIC %md
# MAGIC ### Build fact_campaign
# MAGIC 1. Exploring all tables with campaign description & events
# MAGIC 2. Create fact_campaign, joining or extracting all events relative to campaign
# MAGIC 3. retirve campaign_sk from dim_campaign
# MAGIC 4. create a date_id for log_date
# MAGIC 6. Write the transformed data to gold dim_campaign table
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Set parameter/variable: batch_id

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # call variables from another notebook

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Call the write_to_gold function from another notebook

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

campaign_sku_silver_table = f"{catalog_name}.{silver_schema}.campaign_sku"
campaign_log_silver_table = f"{catalog_name}.{silver_schema}.campaign_log"
dim_campaign_gold_table = f"{catalog_name}.{gold_schema}.dim_campaign"
fact_campaign_gold_table = f"{catalog_name}.{gold_schema}.fact_campaign"



# COMMAND ----------

# read the fact table
campaign_log_df = spark.read.table(campaign_log_silver_table)
#display(campaign_log_df)

# retrieving campaign_sk from dim_campaign
dim_campaign = spark.read.table(dim_campaign_gold_table)
#display(dim_campaign)

# create fact campaign

fct_campaign_df = (
    campaign_log_df.alias("cld").withColumn("campaign_log_date_id",
        F.date_format("campaign_log_date", "yyyyMMdd").cast("int"))
).join(
    dim_campaign.alias("dc"),
    F.col("cld.campaign_id")==F.col("dc.campaign_id"),
    how = 'left'
).select(
    F.col("dc.campaign_sk"),
    F.col("campaign_log_date_id"),
    F.col("cld.campaign_log_date"),
     F.col("cld.campaign_impressions"),
     F.col("cld.campaign_clicks"),
     F.col("cld.campaign_spend"),
     
)

#display(fct_campaign_df)


# COMMAND ----------

# MAGIC %md
# MAGIC # Write the transformed data to gold dim_campaign table

# COMMAND ----------

write_to_gold(
    fct_campaign_df,
    fact_campaign_gold_table,
    "t.campaign_sk = s.campaign_sk AND t.campaign_log_date_id = s.campaign_log_date_id",
    [
       "campaign_log_date_id",
       "campaign_log_date",
       "campaign_impressions",
       "campaign_clicks",
       "campaign_spend",
    ]
)