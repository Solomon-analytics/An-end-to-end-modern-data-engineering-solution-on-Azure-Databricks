# Databricks notebook source
# MAGIC %md
# MAGIC # Transform shipment bronze data
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
# MAGIC > # Notebook: Adding dynamic workspace environment variable

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
# MAGIC # File: shipment
# MAGIC  - define shipment source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_shipment_table = f"{catalog_name}.{bronze_schema}.shipment"
silver_shipment_table = f"{catalog_name}.{silver_schema}.shipment"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

shipment_df = spark.read.table(bronze_shipment_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

shipment_renamed_df = shipment_df.withColumnRenamed("carrier", "shipping_carrier").drop("_loaded_at")

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup loggings for audit purpose:
row_count = shipment_renamed_df.count()

# remove whitespaces from string columns
shipment_rem_space = trim_whitespaces(shipment_renamed_df)

# remove nulls from business keys --> count of rows(15,369) --> shipment_id uniquely represent each row --> combination of all other attributes in this dataset also unqiquly represent each row
#display(shipment_rem_space.count())
#display(shipment_rem_space.groupBy("shipment_id").agg(F.count("*").alias("count")).filter(F.col("count")>1))
#display(shipment_rem_space.groupBy(["order_id", "ship_date", "delivery_date", "shipping_carrier"]).agg(F.count("*").alias("count")).filter(F.col("count")>1))
 
# create a order data quality flag which check if a record contains an order_id
shipment_order_flag = shipment_rem_space.withColumn("order_flag", F.when(F.col("order_id").isNotNull(), "Y").otherwise("N"))

# drop Nulls from shipment_id
shipment_rem_nulls = remove_nulls(shipment_order_flag, ["shipment_id"])
record_count_1 = shipment_rem_nulls.count()
print(f"before nulls removal: {row_count}, after nulls removal: {record_count_1}")

# remove duplicates
shipment_final_df = shipment_rem_nulls.dropDuplicates()
record_count_2 = shipment_final_df.count()
print(f"before duplicates removal: {record_count_1}, after duplicates removal: {record_count_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    shipment_final_df,
    silver_shipment_table,
    "t.shipment_id = s.shipment_id",
    [
        "order_id",
        "ship_date",
        "delivery_date",
        "shipping_carrier",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
        "order_flag"
    ]
)