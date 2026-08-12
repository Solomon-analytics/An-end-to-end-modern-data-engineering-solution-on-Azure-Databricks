# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

silver_cust = f"{catalog_name}.{silver_schema}.cust_master"

TRACKED = ["customer_segment", "customer_city_id", "customer_credit_limit",
           "customer_payment_terms", "customer_active_flag"]

row_hash = F.sha2(
    F.concat_ws("||", *[
        F.coalesce(F.col(c).cast("string"), F.lit("~NULL~")) for c in TRACKED
    ]), 256)

backfilled = (
    spark.read.table(silver_cust)
    .withColumn("row_hash",   row_hash)
    .withColumn("valid_from", F.to_date(F.lit("2024-08-01")))
    .withColumn("valid_to",   F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
)

(backfilled.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(silver_cust))

print(f"backfilled {backfilled.count():,} rows")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total,
# MAGIC COUNT(DISTINCT customer_id) as customers,
# MAGIC SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
# MAGIC COUNT(DISTINCT row_hash) AS distinct_hashes
# MAGIC FROM kestrel_data_eng_prj.silver.cust_master;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, COUNT(*) AS ROWS
# MAGIC FROM kestrel_data_eng_prj.silver.cust_master
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY ROWS DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM kestrel_data_eng_prj.silver.cust_master
# MAGIC WHERE customer_id = '5715'
# MAGIC ORDER BY batch_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE kestrel_data_eng_prj.gold.fact_shipment;