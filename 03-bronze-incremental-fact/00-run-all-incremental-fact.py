# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
print(f"Running silver layer for batch: {v_batch_id}")

# COMMAND ----------

# MAGIC %run ./01.invoice_line

# COMMAND ----------

# MAGIC %run ./02.invoice

# COMMAND ----------

# MAGIC %run ./03.payment

# COMMAND ----------

# MAGIC %run ./04.sales_order_lines

# COMMAND ----------

# MAGIC %run ./05.sales_order

# COMMAND ----------

# MAGIC %run ./06.shipment

# COMMAND ----------

print(f"silver layer complete for batch {v_batch_id}")