# Databricks notebook source
# MAGIC %md
# MAGIC # Transform sales_order_lines bronze data
# MAGIC  - Read file using spark dataframe reader API
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - add data quality flags
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
# MAGIC # File: sales_order_lines
# MAGIC  - define sales_order_lines source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_sales_order_lines_table = f"{catalog_name}.{bronze_schema}.sales_order_lines"
silver_sales_order_lines_table = f"{catalog_name}.{silver_schema}.sales_order_lines"


# COMMAND ----------

# MAGIC %md
# MAGIC  - Read file using spark dataframe reader API

# COMMAND ----------

sales_order_lines_df = spark.read.table(bronze_sales_order_lines_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

sales_order_renamed_df = sales_order_lines_df.withColumnsRenamed({
    "line_id":"order_line_id",
    "sku":"product_sku",
    "quantity":"line_quantity",
    "unit_price":"line_unit_price",
    "discount_pct":"line_discount_pct"
}).drop("_loaded_at")

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

from pyspark.sql.window import Window
# setup loggings for audit purpose
row_count = sales_order_renamed_df.count()

# remove whitespaces from string columns
sales_order_rem_space = trim_whitespaces(sales_order_renamed_df)

# remove nulls from business keys --> row_count(42,055) --> order_line_id uniquely represent each row --> there's one record in this dataset which have the same record across each of the columns apart from order_line_id: considering there are no date attributes in this columns, it's challenging to tell if this is a duplicate(multiple system entry). for this, create a flag:

#display(sales_order_rem_space.count())
#display(sales_order_rem_space.groupBy(["order_line_id"]).agg(F.count("*").alias("count")).filter(F.col("count") > 1))
#display(sales_order_rem_space.groupBy(["order_id", "product_sku"]).agg(F.count("*").alias("count")).filter(F.col("count") > 1)) <<-- 40 occurence
#display(sales_order_rem_space.groupBy(["order_id", "product_sku", "line_discount_pct", "line_quantity"]).agg(F.count("*").alias("count")).filter(F.col("count") > 1))
#display(sales_order_rem_space.filter(F.col("order_id")=="ORD0171833"))

# creating a flag for identical records with varying order_line_id
w = Window.partitionBy("order_id", "product_sku", "line_discount_pct", "line_quantity", "line_unit_price")
sales_order_lines_flag_df = sales_order_rem_space.withColumn("sales_identifcal_flag", F.col("product_sku").isNotNull() & (F.count("*").over(w) > 1))


# check for null in business keys --> there are 634 null records in product_sku --> create a data quality flag for this
#display(sales_order_lines_flag_df.filter(F.col("product_sku").isNull()).count())
sales_order_lines_prd_flag = sales_order_lines_flag_df.withColumn("product_is_null_flag", F.when(F.col("product_sku").isNull(), "Y").otherwise("N"))

# drop null in order_line_id
sales_order_lines_rem_nulls = sales_order_lines_prd_flag.filter(F.col("order_line_id").isNotNull())
new_record_count = sales_order_lines_rem_nulls.count()
print(f"before dropping null:{row_count} | after dropping nulls: {new_record_count}")

# drop nulls
sales_order_lines_final_df = sales_order_lines_rem_nulls.dropDuplicates()
new_record_2 = sales_order_lines_final_df.count()
print(f"before dropping duplicates:{new_record_count} | after dropping duplicates: {new_record_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    sales_order_lines_final_df,
    silver_sales_order_lines_table,
    "t.order_line_id = s.order_line_id",
    [
         'order_id',
         'product_sku',
         'line_quantity',
         'line_unit_price',
         'line_discount_pct',
         'line_total',
         'ingestion_timestamp',
         'source_file',
         'batch_id',
         'sales_identifcal_flag',
         'product_is_null_flag'
    ]
)