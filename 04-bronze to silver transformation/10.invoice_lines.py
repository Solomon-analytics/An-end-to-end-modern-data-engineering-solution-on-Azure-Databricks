# Databricks notebook source
# MAGIC %md
# MAGIC # Transform invoice_lines bronze data
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
# MAGIC # File: invoice_lines
# MAGIC  - define invoice_lines source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_invoice_lines_table = f"{catalog_name}.{bronze_schema}.invoice_lines"
silver_invoice_lines_table = f"{catalog_name}.{silver_schema}.invoice_lines"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

invoice_lines_df = spark.read.table(bronze_invoice_lines_table).filter(F.col("batch_id")==v_batch_id)


# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

# drop column: _loaded_at
invoice_lines_df = invoice_lines_df.drop("_loaded_at")


# rename columns
invoice_lines_renamed_df = invoice_lines_df.withColumnsRenamed({
    "invoice_no":"invoice_number",
    "line_no":"line_number",
    "amount":"invoice_amount"
})


# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup logging for audit purpose
row_count = invoice_lines_renamed_df.count()

# remove whitespaces from string columns
invoice_lines_rem_space = trim_whitespaces(invoice_lines_renamed_df)

# remove nulls from string columns --> count of rows(30,047) --> invoice_number does not distinctly represent each rows
#display(invoice_lines_rem_space.groupBy("invoice_number").count().where(F.col("count") > 1))
#display(invoice_lines_rem_space.filter(F.col("invoice_number")=="INV0000002"))
#display(invoice_lines_rem_space.groupBy("line_id").count().filter(F.col("count") > 1))
# line_id uniquely represent each row - however, it's important to ensure the invoice number does not contain null values
# display(invoice_lines_rem_space.groupBy(["invoice_number", "line_number"]).count().filter(F.col("count") > 1)) uniquely represent each record

# creating incoice_number_null_flag and line_number_null_flag
invoice_lines_flags = invoice_lines_rem_space.withColumn("invoice_number_null_flag", F.when(F.col("invoice_number").isNull(), "Y").otherwise("N")).withColumn("line_number_null_flag", F.when((F.col("line_number").isNull()) | (F.col("line_number") <= 0), "Y").otherwise("N"))

# dropping rows without invoice_number
invoice_lines_rem_nulls = remove_nulls(invoice_lines_flags, ["invoice_number"])

print(f"before removing nulls: {row_count} | after removing nulls: {invoice_lines_rem_nulls.count()}")

# remove duplicates
invoice_lines_rem_dup = remove_duplicates(invoice_lines_rem_nulls, ["line_id"])

print(f" before duplicate: {invoice_lines_rem_nulls.count()} | after duplicates: {invoice_lines_rem_dup.count()}")




# COMMAND ----------

# MAGIC %md
# MAGIC - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    invoice_lines_rem_dup,
    silver_invoice_lines_table,
    "t.line_id = s.line_id",
    [
        "invoice_number",
        "line_number",
        "invoice_amount",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
        'invoice_number_null_flag',
        'line_number_null_flag'
    ]
)