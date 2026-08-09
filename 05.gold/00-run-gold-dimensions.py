# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
print(f"Running silver layer for batch: {v_batch_id}")

# COMMAND ----------

# MAGIC %run ./01.dim_customer

# COMMAND ----------

# MAGIC %run ./02.dim_customer_account

# COMMAND ----------

# MAGIC %run ./03.dim_product

# COMMAND ----------

# MAGIC %run ./04.dim_campaign

# COMMAND ----------

print(f"silver layer complete for batch {v_batch_id}")