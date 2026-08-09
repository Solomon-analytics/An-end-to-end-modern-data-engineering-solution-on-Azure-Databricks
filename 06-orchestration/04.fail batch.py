# Databricks notebook source
# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

if v_batch_id:
    delta_table = DeltaTable.forName(spark, control_table)

    source_df = (
        spark.createDataFrame([(v_batch_id,)], ["batch_id"])
            .withColumn("status", F.lit("failed"))
            .withColumn("updated_timestamp", F.current_timestamp())
            .withColumn("error_message", F.lit(f"Run failed on batch {v_batch_id}"))
    )

    (
        delta_table.alias("t")
            .merge(source_df.alias("s"),
                   "t.batch_id = s.batch_id AND t.status = 'in-progress'")
            .whenMatchedUpdate(set={
                "status":            "s.status",
                "updated_timestamp": "s.updated_timestamp",
                "error_message":     "s.error_message",
            })
            .execute()
    )

    print(f"Batch {v_batch_id} marked as failed")
else:
    raise Exception("batch_id is missing")