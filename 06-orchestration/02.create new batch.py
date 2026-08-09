# Databricks notebook source
# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F
# this tasks runs straight after the conditon check in the 01.identify notebook
if v_batch_id: # is the value that pulled in from previous task
    in_progress_df = (
        spark.createDataFrame(
            [Row(batch_id=v_batch_id, status = "in-progress")]
        )# creates a one-row Dataframe in memory. The lists contains a single row with two fields: the batch id that was passed in, a hardcoded status(in-progress)
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
        # adds two audit columns
    )
    
    (
        in_progress_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(control_table)
    )# writes a Delta table, which transaction log and time travel. mode: append, adds rows without touching anything already there. saveAsTable: registers iin the metastore, saved as control_table

    print(f"Marked batch {v_batch_id} as in-progress")
else:
    raise Exception("batch_id is missing")# if no batch came through, throw an error...