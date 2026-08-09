# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
print(f"Running silver layer for batch: {v_batch_id}")

# COMMAND ----------

# MAGIC %run ./05.fact_campaign

# COMMAND ----------

# MAGIC %run ./06.fact_sales_order

# COMMAND ----------

# MAGIC %run ./07.fact_sales_order_lines

# COMMAND ----------

# MAGIC %run ./08.fact_shipment

# COMMAND ----------

# MAGIC %run ./09.product_campaign_lookup

# COMMAND ----------

print(f"silver layer complete for batch {v_batch_id}")