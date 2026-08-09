# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
print(f"Running silver layer for batch: {v_batch_id}")

# COMMAND ----------

# MAGIC %run ./01.address

# COMMAND ----------

# MAGIC %run ./02.campaign_log

# COMMAND ----------

# MAGIC %run ./03.cust_master

# COMMAND ----------

# MAGIC %run ./04.campaign_sku

# COMMAND ----------

# MAGIC %run ./05.channels

# COMMAND ----------

# MAGIC %run ./06.cities

# COMMAND ----------

# MAGIC %run ./07.customer_contacts

# COMMAND ----------

# MAGIC %run ./08.exchange_rates

# COMMAND ----------

# MAGIC %run ./09.products

# COMMAND ----------

# MAGIC %run ./10.regions

# COMMAND ----------

# MAGIC %run ./11.sales_targets

# COMMAND ----------

# MAGIC %run ./12.subcategories

# COMMAND ----------

# MAGIC %run ./13.user_details

# COMMAND ----------

print(f"silver layer complete for batch {v_batch_id}")