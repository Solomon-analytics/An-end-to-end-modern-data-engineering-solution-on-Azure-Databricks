# Databricks notebook source
# MAGIC %md
# MAGIC ### Build product Dimension
# MAGIC 1. Exploring all tables with product description
# MAGIC 2. Create dim_product, joining tables on the right business key
# MAGIC 3. select only columns that distinctly describe the product
# MAGIC 4. goal is to have a distinct count and description of the product_sku
# MAGIC 5. create a new product_sk column using the hash function --> this later replaces the customer_id in each of the fact tables
# MAGIC 5. Write the transformed data to gold dim_product table
# MAGIC
# MAGIC
# MAGIC

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

# MAGIC %md
# MAGIC %md
# MAGIC # Exploring All product tables in silver layer
# MAGIC  - Understand the grain of each table
# MAGIC  - Understand the business Key in each table
# MAGIC  - Explore table which shares similar business key
# MAGIC  - tables: products & product_sub_categories

# COMMAND ----------

products_silver_table = f"{catalog_name}.{silver_schema}.products"
product_sub_categories_silver_table = f"{catalog_name}.{silver_schema}.product_sub_categories"
dim_product_table = f"{catalog_name}.{gold_schema}.dim_product"


# COMMAND ----------

# read silver product table
product_df = spark.read.table(products_silver_table)
#display(product_df)

# is product_id distinct?
#display(product_df.groupBy("product_sku").count().filter(F.col("count")>1))#product_sku is distinct
# count of rows: 1200 
#display(product_df.count())
# is sub_category_id distinct?


# read silver product_sub_category table
product_sub_category_df = spark.read.table(product_sub_categories_silver_table)
#display(product_sub_category_df)
# is product_sub_category_id distinct?
#display(product_sub_category_df.groupBy("product_sub_category_id").count().filter(F.col("count")>1)) product_sub_category_id is distinct

# retrieve product_sub_category_name and product_category_name from silver product_sub_category table

product_df_final = product_df.alias("p").join(
    product_sub_category_df.alias("psc"),
    F.col("p.sub_category_id")==F.col("psc.product_sub_category_id"),
    "left"
).select(
    F.xxhash64(F.col("p.product_sku")).alias("product_sk"), # add product_sk
    F.col("p.product_sku").alias("product_id"),
    F.col("p.product_name"),
    F.col("p.product_brand"),
    F.round(F.col("p.product_price").cast("double"), 2).alias("product_price"),
    F.round(F.col("p.product_cost").cast("double"), 2).alias("product_cost"),
    F.col("product_supplier"),
    F.col("psc.product_sub_category_name"),
    F.col("psc.product_category_name")
)

#display(product_df_final)




# COMMAND ----------

# MAGIC %md
# MAGIC # Write the transformed data to gold dim_product table

# COMMAND ----------

write_to_gold(
    product_df_final,
    dim_product_table,
    "t.product_sk = s.product_sk",
    [
        "product_id",
        "product_name",
        "product_brand",
        "product_price",
        "product_cost",
        "product_supplier",
        "product_sub_category_name",
        "product_category_name"
    ]
)