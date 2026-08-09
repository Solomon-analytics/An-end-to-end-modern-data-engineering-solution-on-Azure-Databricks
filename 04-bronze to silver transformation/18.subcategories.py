# Databricks notebook source
# MAGIC %md
# MAGIC # Transform subcategories bronze data
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
# MAGIC # File: subcategories
# MAGIC  - define subcategories source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_subcategories_table = f"{catalog_name}.{bronze_schema}.subcategories"
silver_subcategories_table = f"{catalog_name}.{silver_schema}.product_sub_categories"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

product_sub_categories_df = spark.read.table(bronze_subcategories_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

product_sub_categories_renamed = product_sub_categories_df.withColumnsRenamed({
    "subcategory_id": "product_sub_category_id",
    "subcategory_name": "product_sub_category_name",
    "category_name":"product_category_name",
    
})

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup loggings for audit purpose
row_count = product_sub_categories_renamed.count()

# remove whitespaces from string columns
product_sub_categories_rem_space = trim_whitespaces(product_sub_categories_renamed)

# remove nulls from business key --> 18 rows --> product_sub_category_id uniquely represent each record
#display(product_sub_categories_rem_space.count())
#display(product_sub_categories_rem_space.groupBy("product_sub_category_id").count().filter(F.col("count") > 1))

product_sub_categories_rem_nulls = remove_nulls(product_sub_categories_rem_space, ["product_sub_category_id"])
record_count_1 = product_sub_categories_rem_nulls.count()
print(f"before null removal: {row_count} | after null removal: {record_count_1}")

# remove duplicates
product_sub_categories_final_df = product_sub_categories_rem_nulls.dropDuplicates()
record_count_2 = product_sub_categories_final_df.count()
print(f"before deduplication: {record_count_1} | after deduplication: {record_count_2}")


# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    product_sub_categories_final_df,
    silver_subcategories_table,
    "t.product_sub_category_id = s.product_sub_category_id",
    [
        "product_sub_category_name",
        "product_category_name",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)