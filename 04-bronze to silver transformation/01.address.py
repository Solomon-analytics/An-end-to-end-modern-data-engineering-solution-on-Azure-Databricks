# Databricks notebook source
# MAGIC %md
# MAGIC # Transform address bronze data
# MAGIC  - Read file using spark dataframe reader API
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates
# MAGIC  - Transform values in string columns to title_case
# MAGIC  - Apply business transformation rules
# MAGIC  - Write transformed data to silver table
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

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
# MAGIC # Notebook: Adding dynamic add metadata and ingest_to_silver functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # File: Address
# MAGIC  - define address source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_address_table = f"{catalog_name}.{bronze_schema}.address"
silver_address_table = f"{catalog_name}.{silver_schema}.address"


# COMMAND ----------

# MAGIC %md
# MAGIC # Read file using spark dataframe reader API

# COMMAND ----------

address_df = spark.read.table(bronze_address_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

address_stnd_df = address_df.withColumnsRenamed({"postcode":"customer_postal_code", "street": "customer_street", "customer_street": "customer_street", "city_id": "customer_city_id", "address_type": "customer_address_type", "address_id": "customer_address_id"})

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

address_stnd_df = trim_whitespaces(address_stnd_df)

# COMMAND ----------

# set up logging for audit purpose
before_transformation_count = address_stnd_df.count()

# trim whitespaces
address_stnd_df = trim_whitespaces(address_stnd_df)

# remove nulls from business keys: in thie data, customer_address_id customer_id will be considered as the important business key: 
address_rem_nulls = remove_nulls(address_stnd_df, ["customer_address_id", "customer_id"])

print(f"before null removal: {before_transformation_count}, after null removal: {address_rem_nulls.count()}")

# Remove duplicates: the grain: customer_address_id, customer_id and customer_address_type
address_drop_dup = address_rem_nulls.dropDuplicates(["customer_address_id", "customer_id", "customer_address_type"])

print(f"before duplicate removal: {before_transformation_count}, after duplicate removal: {address_rem_nulls.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC - Write transformed data to silver table

# COMMAND ----------

# write address bronze data to silver
write_to_silver(
    address_drop_dup,
    silver_address_table,
    "t.customer_address_id = s.customer_address_id AND t.customer_id = t.customer_id AND t.customer_address_type = s.customer_address_type",
    [
        "customer_street",
        "customer_city_id",
        "customer_postal_code",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)