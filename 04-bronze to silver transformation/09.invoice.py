# Databricks notebook source
# MAGIC %md
# MAGIC # Transform invoice bronze data
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
# MAGIC # File: invoice
# MAGIC  - define invoice source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_invoice_table = f"{catalog_name}.{bronze_schema}.invoice"
silver_invoice_table = f"{catalog_name}.{silver_schema}.invoice"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

invoice_df = spark.read.table(bronze_invoice_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

# drop _loaded_at column and converting customer_id to string type
invoice_df = invoice_df.drop("_loaded_at").withColumn("customer_id", invoice_df["customer_id"].cast("string")).withColumnRenamed("invoice_no", "invoice_number")

# COMMAND ----------

# MAGIC %md
# MAGIC - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup loggings for audit purpose
row_count = invoice_df.count()

# remove whitespace from string columns
invoice_rem_space_df = trim_whitespaces(invoice_df)

# remove nulls from business keys
# exploring -- 1. df has count of 12,115
# display(invoice_rem_space_df.groupBy("invoice_number").agg(F.count("*")).filter(F.col("count(1)")>1)) uniquely represent each row
# the accepted business key in this df will be invoice number
#display(invoice_rem_space_df.groupBy(["order_id", "customer_id"]).agg(F.count("*")).filter(F.col("count(1)")>1))#uniquely represents each row
#display(invoice_rem_space_df.filter(F.col("order_id").isNull()).count()) # no null records
#display(invoice_rem_space_df.filter(F.col("customer_id").isNull()).count()) # no null records
#display(invoice_rem_space_df.filter(F.col("invoice_date").isNull()).count()) # no null records

invoice_rem_nulls = remove_nulls(invoice_rem_space_df, ["invoice_number"])
print(f"before null removal:{row_count} | after null removal:{invoice_rem_nulls.count()}")

# insert order_id and customer_id null record flags and invoice_date

invoice_rem_nulls = invoice_rem_space_df.withColumn("order_id_null", F.when(F.col("order_id").isNull(), 'Y').otherwise('N')).withColumn("customer_id_null", F.when(F.col("customer_id").isNull(), 'Y').otherwise('N')).withColumn("invoice_date_null_flag", F.when(F.col("invoice_number").isNotNull() & F.col("invoice_date").isNull(), 'Y').otherwise('N'))

# remove duplicates
invoice_rem_dup = invoice_rem_nulls.dropDuplicates()
print(f"before dup removal:{invoice_rem_nulls.count()} | after dup removal:{invoice_rem_dup.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    invoice_rem_dup,
    silver_invoice_table,
    "t.invoice_number = s.invoice_number",
    [
        "order_id",
        "customer_id",
        "invoice_date",
        "currency",
        "invoice_total",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
        "order_id_null",
        "customer_id_null",
        "invoice_date_null_flag"
    ]

)