# Databricks notebook source
# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

if v_batch_id: # check, if nothing was passed in, dont proceed
    delta_table = DeltaTable.forName(spark, control_table) # wrap the existing control table in a DeltaTable object

    source_df = (
        spark.createDataFrame([(v_batch_id,)], ["batch_id"]) # builds a dataframe with one row and one column. 'v_batch_id,' is a list containing one tuple, and that tuple contains the batch_id
            .withColumn("status", F.lit("completed"))# adds a second columns. so every row in that columns gets completed
            .withColumn("updated_timestamp", F.current_timestamp())# add a third column, filled with time the query runs
    )

    (
        delta_table.alias("t")
            .merge(
                source_df.alias("s"),
                "t.batch_id = s.batch_id AND t.status = 'in-progress'"
            )
            .whenMatchedUpdate(set={
                "status":"s.status",
                "updated_timestamp":"s.updated_timestamp"
            })
            .execute()
    )

    print(f"Batch {v_batch_id} marked as completed")
else:
    raise Exception("batch_id is missing")