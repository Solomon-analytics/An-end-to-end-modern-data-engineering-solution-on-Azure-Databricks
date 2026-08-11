# Databricks notebook source
# MAGIC %md
# MAGIC # Transform cust_contacts bronze data
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
# MAGIC # File: customer_contacts
# MAGIC  - define customer_contacts source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_customer_contacts_table = f"{catalog_name}.{bronze_schema}.customer_contacts"
silver_customer_contacts_table = f"{catalog_name}.{silver_schema}.customer_contacts"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

customer_contacts_df = spark.read.table(bronze_customer_contacts_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

# dropping column phone
customer_contacts_selected_df = customer_contacts_df.select(
    F.col("cust_id").alias("customer_id"),
    F.col("contact_name").alias("customer_contact_name"),
    F.col("contact_email").alias("customer_contact_email"),
    F.col("is_primary").alias("is_primary_contact"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")

)

# COMMAND ----------

# MAGIC %md
# MAGIC - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup loggings for audit purpose
row_count = customer_contacts_selected_df.count()

# remove whitespace from string columns
customer_contacts_rem_space = trim_whitespaces(customer_contacts_selected_df)

# remove nulls from business key
# check if customer_id uniquely represents each row
#display(customer_contacts_rem_space.groupBy("customer_id").count().filter(F.col("count")>1)) this column uniquely represent each row

# dropping na in customer_id
customer_contacts_drop_nulls = remove_nulls(customer_contacts_rem_space, ["customer_id"])

# dropping duplicates
customer_contacts_drop_dup = customer_contacts_drop_nulls.dropDuplicates()
print(f"before transformation: {row_count} | after transformation: {customer_contacts_drop_dup.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    customer_contacts_drop_dup,
    silver_customer_contacts_table,
    "t.customer_id = s.customer_id",
    [
        "customer_contact_name",
        "customer_contact_email",
        "is_primary_contact",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)