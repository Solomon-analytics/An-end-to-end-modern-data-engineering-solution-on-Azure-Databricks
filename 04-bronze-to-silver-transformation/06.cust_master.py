# Databricks notebook source
# MAGIC %md
# MAGIC # Transform cust_master bronze data
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
# MAGIC # File: cust_master
# MAGIC  - define cust_master source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_cust_master_table = f"{catalog_name}.{bronze_schema}.cust_master"
silver_cust_master_table = f"{catalog_name}.{silver_schema}.cust_master"


# COMMAND ----------

# MAGIC %md
# MAGIC - Read file using spark dataframe reader API

# COMMAND ----------

cust_master_df = spark.read.table(bronze_cust_master_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

cust_master_selected_df = cust_master_df.select(
    F.col("CUST_ID").alias("customer_id"),
    F.col("CUST_NAME").alias("customer_name"),
    F.col("SEGMENT").alias("customer_segment"),
    F.col("CREDIT_LIMIT").alias("customer_credit_limit"),
    F.col("PAYMENT_TERMS").alias("customer_payment_terms"),
    F.col("ACCOUNT_MANAGER").alias("customer_account_manager"),
    F.col("CITY_ID").alias("customer_city_id"),
    F.col("CREATED_DT").alias("customer_created_date"),
    F.col("ACTIVE_FLAG").alias("customer_active_flag"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup logging for audit purpose
row_count = cust_master_selected_df.count()

# remove whitespaces from string columns
cust_master_whitespace = trim_whitespaces(cust_master_selected_df)

# remove nulls from business keys
# check if customer id uniquely represent each row
#display(cust_master_whitespace.groupBy("customer_id").count().filter(F.col("count")>1)) #does not uniquely represent each row
#display(cust_master_whitespace.filter(F.col("customer_id")=="1568"))
# understanding the grain in this table
#display(cust_master_whitespace.groupBy(["customer_id", "customer_payment_terms"]).agg(F.count("*")).filter(F.col("count(1)")>1))# this is not the grain
#display(cust_master_whitespace.groupBy(["customer_id", "customer_created_date"]).agg(F.count("*")).filter(F.col("count(1)")>1)) #--> this is the grain

# removing nulls from these columns - function will be applied separately, we drop nulls in customer_created_at and customer_id
cust_master_drop_na1 = cust_master_whitespace.dropna(subset = ["customer_id"])
cust_master_drop_na2 = cust_master_whitespace.dropna(subset = ["customer_created_date"])
print(f"before dropping na: {row_count} | after dropping cust_id na: {cust_master_drop_na1.count()} | after dropping cust_created_date na: {cust_master_drop_na2.count()}")


# remove dupplicates using columns that uniquely represent each row
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(
    F.col("customer_created_date").desc(),
    F.col("customer_payment_terms").asc(),   # tie-break, keeps it reproducible
)

cust_master_final_df = (
    cust_master_drop_na2
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to silver table

# COMMAND ----------

write_to_silver_scd2(
    cust_master_final_df,
    silver_cust_master_table,
    "customer_id",
    tracked_columns = ["customer_segment", "customer_city_id",
                       "customer_credit_limit", "customer_payment_terms",
                       "customer_active_flag"],
    effective_from  = F.to_date(F.lit(f"{v_batch_id}-01")),
)