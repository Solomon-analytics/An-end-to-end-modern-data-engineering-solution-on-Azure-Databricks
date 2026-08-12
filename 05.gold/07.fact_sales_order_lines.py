# Databricks notebook source
# MAGIC %md
# MAGIC # Creating fact_sales_order_lines
# MAGIC  - the goal is to have all information about an order
# MAGIC  - validate the silver sales_order_lines before building
# MAGIC  - Join silver sales_order_lines to fact_sales_order, dim_product
# MAGIC  - retieve the following, order_sk, product_sk, order_date, order_date_id, customer_sk and bill_to_account_sk
# MAGIC  - rename order_id in silver sales_order_lines as sales_order_number
# MAGIC  - add a business columns: net_line_value
# MAGIC  - validate the final output: row count must be the same as the silver sales_order_lines
# MAGIC
# MAGIC  - write to fact_sales_order

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Set parameter/variable: batch_id

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # call variables from another notebook

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Call the write_to_gold function from another notebook

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

sales_order_gold_table = f"{catalog_name}.{gold_schema}.fact_sales_order"
sales_order_lines_silver_table = f"{catalog_name}.{silver_schema}.sales_order_lines"
dim_product_table = f"{catalog_name}.{gold_schema}.dim_product"
sales_order_lines_gold_table = f"{catalog_name}.{gold_schema}.fact_sales_order_lines"



# COMMAND ----------

sales_order_lines_df = spark.read.table(sales_order_lines_silver_table)
# validation: table has total count(42,055)
#display(sales_order_lines_df.count())
#display(sales_order_lines_df)
#display(sales_order_lines_df.groupBy("order_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1))
#display(sales_order_lines_df.filter(F.col("order_id")=="ORD0000268"))

# read fact_sales_order_gold_table and retrieve, order_date, order_date_id, order_sk, customer_sk, account, bill_to_account_sk
fact_sales_order_df = spark.read.table(sales_order_gold_table)
#display(fact_sales_order_df)

# read dim_product and retrieve, product_sk, product_number
dim_product = spark.read.table(dim_product_table)
#display(dim_product)

sales_order_lines_selected_df = (
    sales_order_lines_df.alias("sol")
    .join(fact_sales_order_df.alias("so"), F.col("sol.order_id") == F.col("so.order_number"), "left")
    .join(dim_product.alias("p"), F.col("p.product_id")==F.col("sol.product_sku"), "left")
    .select(
        F.xxhash64(F.col("sol.order_line_id")).alias("order_line_sk"),
        F.col("sol.order_id").alias("sales_order_number"),
        F.col("sol.product_sku").alias("product_number"),
        F.col("sol.line_quantity"),
        F.col("sol.line_unit_price"),
        F.col("sol.line_discount_pct"),
        F.round(F.col("sol.line_quantity")*F.col("sol.line_unit_price")*(1-F.coalesce(F.col("sol.line_discount_pct"), F.lit(0))), 2).alias("net_line_value"),
        F.col("sol.line_total"),
        F.col("p.product_sk"),
        F.col("so.customer_sk"),
        F.col("so.bill_to_account_sk"),
        F.col("so.order_sk"),
        F.col("so.order_status").alias("order_line_status"),
        F.col("so.order_date"),
        F.col("so.order_date_id")
    )
)

#display(sales_order_lines_selected_df.count()) validation passed 42,055 count
#display(sales_order_lines_selected_df)
#sales_order_lines_selected_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # write to gold fact_sales_order_lines table

# COMMAND ----------

write_to_gold(
    sales_order_lines_selected_df,
    sales_order_lines_gold_table,
    "t.order_line_sk = s.order_line_sk",
    [
        'sales_order_number',
        'product_number',
        'line_quantity',
        'line_unit_price',
        'line_discount_pct',
        'line_total',
        'net_line_value',
        'product_sk',
        'customer_sk',
        'bill_to_account_sk',
        'order_sk',
        'order_line_status',
        'order_date',
        'order_date_id'
    ]
)