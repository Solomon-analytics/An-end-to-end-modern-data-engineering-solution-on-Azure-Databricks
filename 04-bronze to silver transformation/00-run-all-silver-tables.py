# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
print(f"Running silver layer for batch: {v_batch_id}")

# COMMAND ----------

# MAGIC %run ./01.address

# COMMAND ----------

# MAGIC %run ./02.campaign_log

# COMMAND ----------

# MAGIC %run ./03.campaign_sku

# COMMAND ----------

# MAGIC %run ./04.channel

# COMMAND ----------

# MAGIC %run ./05.cities

# COMMAND ----------

# MAGIC %run ./06.cust_master

# COMMAND ----------

# MAGIC %run ./07.customer_contacts

# COMMAND ----------

# MAGIC %run ./08.exchange_rates

# COMMAND ----------

# MAGIC %run ./09.invoice

# COMMAND ----------

# MAGIC %run ./10.invoice_lines

# COMMAND ----------

# MAGIC %run ./11.payment

# COMMAND ----------

# MAGIC %run ./12.products

# COMMAND ----------

# MAGIC %run ./13.regions

# COMMAND ----------

# MAGIC %run ./14.sales_order

# COMMAND ----------

# MAGIC %run ./15.sales_order_lines

# COMMAND ----------

# MAGIC %run ./16.sales_target

# COMMAND ----------

# MAGIC %run ./17.shipment

# COMMAND ----------

# MAGIC %run ./18.subcategories

# COMMAND ----------

# MAGIC %run ./19.user_details

# COMMAND ----------

print(f"silver layer complete for batch {v_batch_id}")