# Databricks notebook source
# MAGIC %md
# MAGIC # Creating fact_sales_orders
# MAGIC  - the goal is to create a detailed/distinct information about an order
# MAGIC  - validate the silver sales_order before building
# MAGIC  - pre-aggregate the payment_df before including in the join
# MAGIC  - generate the order surrogate key, order_sk
# MAGIC  - join all supporting tables, all left joins so no order is lost
# MAGIC  - standardsise invoice_total to GBP
# MAGIC  - derive addition date attributes
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

sales_order_silver_table = f"{catalog_name}.{silver_schema}.sales_order"
payment_silver_table = f"{catalog_name}.{silver_schema}.payment"
dim_customer_table = f"{catalog_name}.{gold_schema}.dim_customer"
dim_product_table = f"{catalog_name}.{gold_schema}.dim_product"
invoice_silver_table = f"{catalog_name}.{silver_schema}.invoice"
shipment_silver_table = f"{catalog_name}.{silver_schema}.shipment"
exchange_rates_silver_table = f"{catalog_name}.{silver_schema}.exchange_rates"
channels_silver_table = f"{catalog_name}.{silver_schema}.channels"
dim_customer_account_table = f"{catalog_name}.{gold_schema}.dim_customer_account"
#dim_campaign_gold_table = f"{catalog_name}.{gold_schema}.dim_campaign"
fact_sales_order_gold_table = f"{catalog_name}.{gold_schema}.fact_sales_order"



# COMMAND ----------

# exchange rate df
exchange_rates_df = spark.read.table(exchange_rates_silver_table)
#display(exchange_rates_df)

# invoice_df
invoice_df = spark.read.table(invoice_silver_table)
#display(invoice_df)

# dim_customer
dim_customer_df = spark.read.table(dim_customer_table)
#display(dim_customer_df)

# dim_customer_account
dim_customer_account = spark.read.table(dim_customer_account_table)
#display(dim_customer_account)

# channels_df
channels_df = spark.read.table(channels_silver_table)
#display(channels_df)

# payment_df
payment_df = spark.read.table(payment_silver_table)
#display(payment_df)

# sales_order_df
sales_order_df = spark.read.table(sales_order_silver_table)
#display(sales_order_df)
# validation: 84,256 rows across all batches loaded so far
#display(sales_order_df.count())
# order number is distinct
#display(sales_order_df.groupBy("order_number").agg(F.count("*").alias("count")).filter(F.col("count") > 1))
# order number is not null
#display(sales_order_df.filter(F.col("order_number").isNull()).count())


# building the fact_sales_order_df
sales_order_selected_df = (
    sales_order_df.alias("so")
    .withColumn("order_sk", F.xxhash64(F.col("order_number").cast("string")))

    .join(invoice_by_order.alias("i"),
          F.col("so.order_number") == F.col("i.order_id"), "left")

    .join(F.broadcast(exchange_rates_df.alias("e")),
          (F.col("i.currency") == F.col("e.currency"))
          & (F.col("i.invoice_date").cast("date") >= F.col("e.rate_month").cast("date"))
          & (F.col("i.invoice_date").cast("date") < F.add_months(F.col("e.rate_month"), 1)),
          "left")

    # point-in-time join: picks the customer version valid on the order date
    .join(dim_customer_df.alias("dc"),
          (F.col("so.customer_id") == F.col("dc.customer_id"))
          & (F.to_date(F.col("so.order_date")) >= F.col("dc.valid_from"))
          & (F.to_date(F.col("so.order_date")) < 
             F.coalesce(F.col("dc.valid_to"), F.lit("2999-12-31").cast("date"))),
          "left")

    # Type 1 dimension, already filtered to is_current

    .join(dim_customer_account.alias("ac"),
          F.col("so.customer_id") == F.col("ac.account_id"), "left")

    .join(F.broadcast(channels_df.alias("c")),
          F.col("so.channel_code") == F.col("c.channel_code"), "left")

    .join(payment_by_invoice.alias("p"),
          F.col("i.invoice_number") == F.col("p.invoice_number"), "left")

    .select(
        F.col("order_sk"),
        F.col("so.order_number"),
        F.to_date(F.col("so.order_date")).alias("order_timestamp"),
        F.col("dc.customer_sk"),
        F.col("dc.customer_name"),
        F.col("ac.bill_to_account_sk"),
        F.col("ac.account_payment_terms"),
        F.col("so.order_status"),
        F.col("c.channel_name"),
        F.col("p.payment_method"),
        F.to_date(F.col("i.invoice_date")).alias("invoice_timestamp"),
        F.col("p.first_payment_date").alias("payment_timestamp"),
        F.round(F.col("i.invoice_total") * F.col("e.rate_to_gbp"), 2).alias("order_total_gbp")
    )
    .withColumn("order_date",      F.to_date(F.col("order_timestamp")))
    .withColumn("invoice_date",    F.to_date(F.col("invoice_timestamp")))
    .withColumn("payment_date",    F.to_date(F.col("payment_timestamp")))
    .withColumn("order_date_id",   F.date_format(F.col("order_timestamp"),   "yyyyMMdd"))
    .withColumn("invoice_date_id", F.date_format(F.col("invoice_timestamp"), "yyyyMMdd"))
    .withColumn("payment_date_id", F.date_format(F.col("payment_timestamp"), "yyyyMMdd"))
)

# validation
#display(sales_order_selected_df.count())  # expect 84,256
#display(sales_order_selected_df.groupBy("order_sk").agg(F.count("*").alias("count")).filter(F.col("count") > 1))  # expect empty


# COMMAND ----------

# MAGIC %md
# MAGIC # write to gold: fact_sales_order

# COMMAND ----------

write_to_gold(
    sales_order_selected_df,
    fact_sales_order_gold_table,
    "s.order_sk = t.order_sk",
    [
        'order_number',
        'order_timestamp',
        'customer_sk',
        'customer_name',
        'bill_to_account_sk',
        'account_payment_terms',
        'order_status',
        'channel_name',
        'payment_method',
       'invoice_timestamp',
       'payment_timestamp',
       'order_total_gbp',
       'order_date',
       'invoice_date',
       'payment_date',
       'order_date_id',
      'invoice_date_id',
       'payment_date_id'
    ]

)