# Databricks notebook source
# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Create control_schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{control_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC - create batch_control table in control_schema

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {catalog_name}.{control_schema}.batch_control
          (
              batch_id STRING,
              status STRING,
              created_timestamp TIMESTAMP,
              updated_timestamp TIMESTAMP,
              error_message STRING
          )""")

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"
display(spark.read.table(control_table))