# Databricks notebook source
# MAGIC %md
# MAGIC ### Build dim_campaign
# MAGIC 1. Exploring all tables with campaign description
# MAGIC 2. Create dim_campaign, joining or extracting description from fact
# MAGIC 3. select only columns that distinctly describe the campaign
# MAGIC 4. goal is to have a distinct count and description of the campaign_id
# MAGIC 5. Add the following business columns: campaign_sk, campaign_duration_days, campaign_start_date_id, campaign_end_date_id
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
dim_campaign_table = f"{catalog_name}.{gold_schema}.dim_campaign"


# COMMAND ----------

# MAGIC %md
# MAGIC - exploring both tables: campaign_log(fact) and campaigh_sku(dim)

# COMMAND ----------

# exploring campaigh skuyhtgrfdsaz\ewqa 
#campaign_sku = spark.read.table(campaign_sku_silver_table)
#display(campaign_sku)
# campaign_id does not uniquely represent each record in this df
#display(campaign_sku.groupBy('campaign_id').agg(F.count('campaign_sku').alias("count")))
# there are multiple campign_sku related to a campaign_id
# are there multiple campaign_id related to a sku?
#display(campaign_sku.groupBy('campaign_sku').agg(F.count('campaign_id').alias("count"))) there are also multiple campaign_id related to a campaign_sku

# exploring campaign_log
campaign_log = spark.read.table(campaign_log_silver_table)
#display(campaign_log)
# this table contains vital business information as compared to the campaign_sku
# campaign_id is not distinct. reason: campaign_id is linked with multiple campaign_budget, campaign_start_date and campaign_end_date
# retrieving descriptive information fro campaign_log
# goal: campaign_id, campaign_name must be unique to each record

campaign_log_selected = (
    campaign_log.groupBy(
        "campaign_id",
        "campaign_name",
        "campaign_channel"
    ).agg(
        F.avg(F.col("campaign_budget")).alias("total_campaign_budget"),
        F.min(F.col("campaign_start_date")).alias("campaign_start_date"),
        F.max(F.col("campaign_end_date")).alias("campaign_end_date")
    )
    .withColumn("campaign_sk", F.xxhash64(F.col("campaign_id").cast("string")))
    .withColumn("campaign_duration_days", F.datediff(F.col("campaign_end_date"), F.col("campaign_start_date")) + 1)
    .withColumn("campaign_start_date_id", F.date_format(F.col("campaign_start_date"), "yyyyMMdd").cast("int"))
    .withColumn("campaign_end_date_id", F.date_format(F.col("campaign_end_date"), "yyyyMMdd").cast("int"))
)
#display(campaign_log_selected)
#campaign_log_selected.columns


# COMMAND ----------

# MAGIC %md
# MAGIC # Write the transformed data to gold dim_campaign table

# COMMAND ----------

write_to_gold(
    campaign_log_selected,
    dim_campaign_table,
    "t.campaign_sk = s.campaign_sk",
    [
        'campaign_id',
        'campaign_name',
        'campaign_channel',
        'total_campaign_budget',
        'campaign_start_date',
        'campaign_end_date',
        'campaign_duration_days',
        'campaign_start_date_id',
        'campaign_end_date_id'
    ]
)