# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest sales_order parquet file
# MAGIC - considering file is saved in a batch folder, set a parameter, batch_id
# MAGIC - Read the files using spark dataframe reader API
# MAGIC - Add metadata Columns
# MAGIC     - Source file 
# MAGIC     - ingestion timestamp
# MAGIC - Write all files to its bronze delta table
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md
# MAGIC # set parameter and variable: batch_id

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic workspace environment variable

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add_ingestion_timestamp and ingest_to_bronze functions

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: sales orders parquet
# MAGIC  - define sales orders source file and bronze table name using the environment variable
# MAGIC

# COMMAND ----------

sales_order_source_file = f"{landing_folder_path}/{v_batch_id}/{sales_order}"
bronze_sales_order_name = f"{catalog_name}.{bronze_schema}.sales_order"

# COMMAND ----------

# MAGIC %md
# MAGIC # create sales_order schema
# MAGIC

# COMMAND ----------

sales_order_schema = StructType(fields=[
    StructField('ORDER_NO', StringType()),
    StructField('ORD_DT', TimestampType()),
    StructField('CUST_ID', LongType()),
    StructField('ORD_STATUS', StringType()),
    StructField('PRIORITY_CD', StringType()),
    StructField('CHANNEL_CD', LongType()),
    StructField('BILL_ADDR_ID', LongType()),
    StructField('order_source', StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the sales_order table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# Read the sales_order table with its defined schema
sales_order_df = spark.read.format('parquet').option('header', True).schema(sales_order_schema).option('mode', 'FAILFAST').load(sales_order_source_file)

# add ingestion metadata
sales_order_final_df = add_ingestion_metadata(sales_order_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    sales_order_final_df,
    bronze_sales_order_name,
    v_batch_id
)