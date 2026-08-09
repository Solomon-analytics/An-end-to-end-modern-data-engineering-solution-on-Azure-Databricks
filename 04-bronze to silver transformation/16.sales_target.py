# Databricks notebook source
# MAGIC %md
# MAGIC # Transform sales_targets bronze data
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
# MAGIC # File: sales_targets
# MAGIC  - define sales_targets source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_sales_targets_table = f"{catalog_name}.{bronze_schema}.sales_targets"
silver_sales_targets_table = f"{catalog_name}.{silver_schema}.sales_targets"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

sales_targets_df = spark.read.table(bronze_sales_targets_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

sales_targets_renamed_df = sales_targets_df.withColumnsRenamed({
    "target_month":"sales_target_month",
    "region_id":"sales_region_id",
    "target_revenue":"sales_target_revenue"
})

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup loggings for audit purpose
row_count = sales_targets_renamed_df.count()

# remove nulls from business keys ->there are no nulls
#display(sales_targets_renamed_df.filter(F.col("sales_target_month").isNull()).count())
#display(sales_targets_renamed_df.filter(F.col("region_id").isNull()).count())
sales_targets_rem_nulls = remove_nulls(sales_targets_renamed_df, ["sales_target_month", "sales_region_id"])
record_count_1 = sales_targets_rem_nulls.count()
print(f"before dropping nulls: {row_count} | after dropping nulls: {record_count_1}")

# remove duplicates - there are no duplicates
#display(sales_targets_rem_nulls.groupBy(["sales_target_month", "sales_region_id"]).agg(F.count("*").alias("count")).filter(F.col("count") > 1))

sales_targets_final_df = sales_targets_rem_nulls.dropDuplicates(subset=["sales_target_month", "sales_region_id"])
record_count_2 = sales_targets_final_df.count()
print(f"before dropping duplicates: {record_count_1} | after dropping duplicates: {record_count_2}")


# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    sales_targets_final_df,
    silver_sales_targets_table,
    "t.sales_target_month = s.sales_target_month AND t.sales_region_id = s.sales_region_id",
    [
        "sales_target_revenue",
        "ingestion_timestamp",
        "source_file",
        "batch_id"

    ]
)