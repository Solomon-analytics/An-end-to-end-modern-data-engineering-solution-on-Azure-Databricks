# Databricks notebook source
# MAGIC %md
# MAGIC # call variable from another notebook

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

from pyspark.sql import functions as F

# get batch folders from landing
landing_batches = sorted([
    file.name.rstrip("/")
    for file in dbutils.fs.ls(landing_folder_path)
    if file.isDir()
]) # this lists what is in the landing folder - isDIR: keeps only folders, Sort-order them alphabetically

# Read tracked batches
if spark.catalog.tableExists(control_table):
    tracked_batches = [
        row.batch_id
        for row in (
            spark.table(control_table)
                .filter(F.col("status").isin("in-progress", "completed"))
                .select("batch_id")
                .distinct()
                .collect()
        ) # spark.table(control_table) opens the control/audit table as a DataFrame. filter keeps only batches that are either running or done
        # select and distinct: trims to one column and dedup. collect: pulls the rows back to the dtiver as python object
    ]
else:
    tracked_batches = []


# identify earliest unprocessed batch
new_batches = sorted(list(set(landing_batches) - set(tracked_batches))) # everything in landing that is not already tracked. converted to a sorted list
next_batch = new_batches[0] if new_batches else None # takes the first unprocessed batch, or None if there is nothing to do

print(f"landing batches: {landing_batches}") # visibility
print(f"tracked batches: {tracked_batches}") # visibility
print(f"next batch to process: {next_batch}") # visibility

if next_batch is None: # next batch set earlier to either batch id string or None. None means it came back empty
    dbutils.jobs.taskValues.set(key="p_batch_id", value="") # writes a key value pair into Databricks job run history.
    dbutils.jobs.taskValues.set(key="has_batch", value="false") # there is no new batch to process
else:
    dbutils.jobs.taskValues.set(key="p_batch_id", value=next_batch) # publishes the actual batch id. the ingestion task downstream reads this and uses it to build the source path it should read from
    dbutils.jobs.taskValues.set(key="has_batch", value="true") # there is a batch waiting to be processed