# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest CUST_MASTER csv file
# MAGIC - considering file is saved in a batch folder, set a parameter, batch_id
# MAGIC - Read the files using spark dataframe reader API
# MAGIC - Add metadata Columns
# MAGIC     - Source file 
# MAGIC     - ingestion timestamp
# MAGIC - Write all files to its bronze delta table

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: Adding dynamic add_ingestion_timestamp and ingest_to_bronze functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: CUST_MASTER.csv
# MAGIC  - define CUST_MASTER source file and bronze table name using the environment variable
# MAGIC

# COMMAND ----------

cust_master_source_file = f"{landing_folder_path}/{v_batch_id}/CUST_MASTER.csv"
bronze_cust_master_name = f"{catalog_name}.{bronze_schema}.cust_master"


# COMMAND ----------

# MAGIC %md
# MAGIC # create cust_master schema
# MAGIC

# COMMAND ----------

cust_master_schema = StructType(fields=[
    StructField("CUST_ID", StringType()),
    StructField("CUST_NAME", StringType()),
    StructField("SEGMENT", StringType()),
    StructField("CREDIT_LIMIT", IntegerType()),
    StructField("PAYMENT_TERMS", StringType()),
    StructField("ACCOUNT_MANAGER", StringType()),
    StructField("CITY_ID", StringType()),
    StructField("CREATED_DT", DateType()),
    StructField("ACTIVE_FLAG", StringType())
]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the cust_master table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# read cust_master table with its defined schema
cust_master_df = spark.read.format('csv').option('header', True).schema(cust_master_schema).option('mode', 'FAILFAST').load(cust_master_source_file)

# add ingestion_metadata
cust_master_final_df = add_ingestion_metadata(cust_master_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    cust_master_final_df,
    bronze_cust_master_name,
    v_batch_id
)