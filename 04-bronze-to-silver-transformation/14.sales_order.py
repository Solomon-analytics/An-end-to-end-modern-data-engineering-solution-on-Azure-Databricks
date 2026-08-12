# Databricks notebook source
# MAGIC %md
# MAGIC # Transform sales_order bronze data
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
# MAGIC # File: sales_order
# MAGIC  - define sales_order source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_sales_order_table = f"{catalog_name}.{bronze_schema}.sales_order"
silver_sales_order_table = f"{catalog_name}.{silver_schema}.sales_order"


# COMMAND ----------

# MAGIC %md
# MAGIC - Read file using spark dataframe reader API

# COMMAND ----------

sales_order_df = spark.read.table(bronze_sales_order_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

sales_order_rename_df = sales_order_df.withColumnsRenamed({
    "ORDER_NO":"order_number",
    "ORD_DT":"order_date",
    "CUST_ID":"customer_id",
    "ORD_STATUS":"order_status",
    "PRIORITY_CD":"priority_code",
    "CHANNEL_CD":"channel_code",
    "BILL_ADDR_ID":"bill_address_id"
}).withColumn(
    "priority_code",
    F.when(F.col("priority_code") == "EXP", "Express")
     .when(F.col("priority_code") == "STD", "Standard")
     .otherwise(F.col("priority_code"))
).withColumn("customer_id", F.col("customer_id").cast("string"))\
 .withColumn("channel_code", F.col("channel_code").cast("string"))\
 .withColumn("order_status", F.initcap(F.col("order_status")))


# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup logging for audit purpose
row_count = sales_order_rename_df.count()

# remove whitespaces from string columns
sales_order_rem_space = trim_whitespaces(sales_order_rename_df)

# remove nulls from business keys --> row count(16,966) --> order_number uniquely represent each record
#display(sales_order_rem_space.count())
#display(sales_order_rename_df.groupBy("order_number").agg(F.count("*").alias("count")).filter(F.col("count") > 1))

sales_order_rem_nulls = remove_nulls(sales_order_rem_space, ["order_number"])
print(f"before null removal: {row_count}, after null removal: {sales_order_rem_nulls.count()}")

# drop duplicates
sales_order_final_df = sales_order_rem_nulls.dropDuplicates()
print(f"before deduplication: {sales_order_rem_nulls.count()}, after deduplication: {sales_order_final_df.count()}")

# Adding order_date and customer_id nulls flag
sales_order_final_df = sales_order_final_df.withColumn("order_date_nulls", F.when(F.col("order_date").isNull(), "Y").otherwise("N"))\
                                          .withColumn("customer_id_nulls", F.when(F.col("customer_id").isNull(), "Y").otherwise("N"))


# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    sales_order_final_df,
    silver_sales_order_table,
    "t.order_number = s.order_number",
    [
        "order_date",
        "customer_id",
        "order_status",
        "priority_code",
        "channel_code",
        "bill_address_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
        "order_date_nulls",
        "customer_id_nulls"
    ]
)