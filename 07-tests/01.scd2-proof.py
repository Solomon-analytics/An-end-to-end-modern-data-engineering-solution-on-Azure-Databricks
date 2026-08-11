# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 proof
# MAGIC  - Evidence that point-in-time joins preserve historical reporting.
# MAGIC  - Ran once, after the batches have loaded. Not part of the job

# COMMAND ----------

# MAGIC %md
# MAGIC - call environment configuration helper

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration

# COMMAND ----------

# MAGIC %md
# MAGIC # Finding customers that changed

# COMMAND ----------

from pyspark.sql import functions as F

changed = spark.sql(f"""
    SELECT customer_id, COUNT(*) AS versions
    FROM {catalog_name}.{gold_schema}.dim_customer
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    ORDER BY versions DESC
    LIMIT 10
""")
display(changed)

# COMMAND ----------

display(spark.sql(f"""
    SELECT customer_id, customer_region_name, customer_segment,
           valid_from, valid_to, is_current
    FROM   {catalog_name}.{gold_schema}.dim_customer
    WHERE  customer_id = '1224'
    ORDER  BY valid_from
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_rows,
           COUNT(DISTINCT customer_id) AS customers,
           SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions,
           SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS closed_versions
    FROM   {catalog_name}.{gold_schema}.dim_customer
"""))