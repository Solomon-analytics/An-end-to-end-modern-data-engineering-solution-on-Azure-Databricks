# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest customer_contacts csv file
# MAGIC - considering file is saved in a batch folder, set a parameter, batch_id
# MAGIC - Read the files using spark dataframereader API
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
# MAGIC

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # File: customer_contacts.csv
# MAGIC  - define customer_contacts source file and bronze table name using the environment variable
# MAGIC
# MAGIC

# COMMAND ----------

customer_contacts_source_file = f"{landing_folder_path}/{v_batch_id}/customer_contacts.csv"
bronze_customer_contacts_name = f"{catalog_name}.{bronze_schema}.customer_contacts"

# COMMAND ----------

# MAGIC %md
# MAGIC # create customer_contacts schema
# MAGIC

# COMMAND ----------

customer_contacts_schema = StructType(fields =[
    StructField('cust_id', StringType()),
    StructField('contact_name', StringType()),
    StructField('contact_email', StringType()),
    StructField('phone', StringType()),
    StructField('is_primary', StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the customer_contacts table with its defined schema
# MAGIC # add ingestion_metadata
# MAGIC # write to bronze table
# MAGIC

# COMMAND ----------

# read customer_contacts table with its defined schema
customer_contacts_df = spark.read.format('csv').option("header", True).schema(customer_contacts_schema).option('mode', 'FAILFAST').load(customer_contacts_source_file)

# Add ingestion metadata
customer_contacts_final_df = add_ingestion_metadata(customer_contacts_df)

# write to bronze, adding the parameter/variable batch_id column
write_to_bronze(
    customer_contacts_final_df,
    bronze_customer_contacts_name,
    v_batch_id
)