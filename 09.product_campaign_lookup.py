# Databricks notebook source
# MAGIC %md
# MAGIC # Creating bridge_campaign_product
# MAGIC  - read campaign_sku from silver
# MAGIC  - validate the source before building
# MAGIC  - read dim_campaign from gold layer
# MAGIC  - read dim_product from the gold layer
# MAGIC  - left join dim_campaign on campaign_id and dim_product om product_number
# MAGIC  - retrieve both surrogate keys campaign_sk and product_sk, as the bridge purpose is to connect the two dimensions
# MAGIC  - write to gold table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

campaign_sku_silver_table = f"{catalog_name}.{silver_schema}.campaign_sku"
dim_campaign = f"{catalog_name}.{gold_schema}.dim_campaign"
dim_product_table = f"{catalog_name}.{gold_schema}.dim_product"
bridge_campaign_product = f"{catalog_name}.{gold_schema}.bridge_campaign_product"




# COMMAND ----------

campaign_sku = spark.read.table(campaign_sku_silver_table)
#display(campaign_sku.count())# 377 count
dim_campaign_df = spark.read.table(dim_campaign)
dim_product_df  = spark.read.table(dim_product_table)

bridge_campaign_product_df = (
    campaign_sku.alias("cs")
    .join(dim_campaign_df.alias("dc"), F.col("dc.campaign_id") == F.col("cs.campaign_id"), "left")
    .join(dim_product_df.alias("p"),   F.col("p.product_id") == F.col("cs.campaign_sku"), "left")
    .select(
        F.col("dc.campaign_sk"),
        F.col("p.product_sk")
    )
)

#display(bridge_campaign_product_df)
# display(bridge_campaign_product_df.count()) #count 377


# COMMAND ----------

# MAGIC %md
# MAGIC # write to gold table

# COMMAND ----------

write_to_gold(
    bridge_campaign_product_df,
    bridge_campaign_product,
    "t.campaign_sk=s.campaign_sk AND t.product_sk=s.product_sk",
    [
      
    ]
)