# Databricks notebook source
# MAGIC %md
# MAGIC # Transform payment bronze data
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
# MAGIC # File: payment
# MAGIC  - define payment source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_payment_table = f"{catalog_name}.{bronze_schema}.payment"
silver_payment_table = f"{catalog_name}.{silver_schema}.payment"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

payment_df = spark.read.table(bronze_payment_table).filter(F.col("batch_id") == v_batch_id)


# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

# rename and drop unwanted column
payment_stnd_df = payment_df.withColumnsRenamed({
    "pay_date":"payment_date",
    "amount":"payment_amount",
    "method":"payment_method",
    "invoice_no":"invoice_number"
}).drop("_loaded_at")



# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup logging for audit purpose
row_count = payment_stnd_df.count()

# remove whitespace from string columns
payment_rem_space = trim_whitespaces(payment_stnd_df)

# remove nulls from business keys --> count of rows(1400) --> payment_id uniquely represent each record
#display(payment_rem_space.groupBy("payment_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1))
payment_rem_nulls = remove_nulls(payment_rem_space, ["payment_id"])
print(f" before null removal: {row_count} | after nulls removal: {payment_rem_nulls.count()}")

# creating invoice_number, payment_date, payment_amount null flags
payment_flag_df = payment_rem_nulls.withColumn("invoice_number_null", F.when(F.col("invoice_number").isNull(), "Y").otherwise("N")).withColumn("payment_date_null", F.when(F.col("payment_date").isNull(), "Y").otherwise("N")).withColumn("payment_amount_null", F.when((F.col("payment_amount").isNull()) | (F.col("payment_amount") <= 0), "Y").otherwise("N"))

# remove duplicates
payment_final_df = payment_flag_df.dropDuplicates()
row_count_1 = payment_final_df.count()
print(f"before dropping duplicates: {payment_rem_nulls.count()} | after dropping duplicates: {payment_final_df.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    payment_final_df,
    silver_payment_table,
    "t.payment_id = s.payment_id",
    [
        'invoice_number',
        'payment_date',
        'payment_amount',
        'payment_method',
        'ingestion_timestamp',
        'source_file',
         'batch_id',
         'invoice_number_null',
         'payment_date_null',
         'payment_amount_null'
    ]
)