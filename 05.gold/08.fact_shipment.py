# Databricks notebook source
# MAGIC %md
# MAGIC # Creating fact_shipment
# MAGIC  - the goal is to have all shipment information of an order
# MAGIC  - validate the grain: shipment_id is unique and not-null
# MAGIC  - read fact_sales_order to inherit order-level context down to shipment grain
# MAGIC  - left join on order number, so a shipment with no order number survives
# MAGIC  - generate shipment_sk as a hash of shipment_id
# MAGIC  - retrieve order_sk from fact_sales_order as a foreign key
# MAGIC  - retrieve customer_sk and bill_to_account_sk: rename customer_sk to ship_co_customer_sk
# MAGIC  - derive three date_id in yyyyMMdd format
# MAGIC  - add three cycle time measures: order to ship, transit, and total orders to delivery
# MAGIC  - add delivery status flags, so an undelivered shipment reads as "in transit" rather than unexpalined null
# MAGIC  - add split-shipment context per order: shipment count, sequence, and flags for split, multi-carrier and final shipment
# MAGIC
# MAGIC  - write to fact_sales_order

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

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
shipment_silver_table = f"{catalog_name}.{silver_schema}.shipment"
shipment_gold_table = f"{catalog_name}.{gold_schema}.fact_shipment"



# COMMAND ----------

shipment_df = spark.read.table(shipment_silver_table)
#display(shipment_df.count())# count 15,369
fact_sales_order_df  = spark.read.table(sales_order_gold_table)

# validation: shipment_id is the grain
# display(shipment_df.groupBy("shipment_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1))
# display(shipment_df.filter(F.col("shipment_id").isNull()).count())

by_order = Window.partitionBy("sales_order_number")
by_order_seq = by_order.orderBy(F.col("ship_date").asc(), F.col("shipment_number").asc())

fact_shipment_df = (
    shipment_df.alias("sh")
    .join(fact_sales_order_df.alias("so"),
          F.col("sh.order_id") == F.col("so.order_number"), "left")
    .select(
        F.xxhash64(F.col("sh.shipment_id").cast("string")).alias("shipment_sk"),
        F.col("sh.shipment_id").alias("shipment_number"),
        F.col("so.order_sk"),
        F.col("sh.order_id").alias("sales_order_number"),
        F.col("so.customer_sk").alias("ship_to_customer_sk"),
        F.col("sh.shipping_carrier"),
        F.to_date(F.col("so.order_date")).alias("order_date"),
        F.to_date(F.col("sh.ship_date")).alias("ship_date"),
        F.to_date(F.col("sh.delivery_date")).alias("delivery_date"),
        F.col("sh.batch_id"),
    )

    # date keys for the date dimension
    .withColumn("order_date_id", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("ship_date_id", F.date_format("ship_date", "yyyyMMdd").cast("int"))
    .withColumn("delivery_date_id", F.date_format("delivery_date", "yyyyMMdd").cast("int"))

    # cycle time measures
    .withColumn("order_to_ship_days", F.datediff("ship_date",     "order_date"))
    .withColumn("transit_days", F.datediff("delivery_date", "ship_date"))
    .withColumn("order_to_delivery_days", F.datediff("delivery_date", "order_date"))

    # delivery status
    .withColumn("is_delivered", F.col("delivery_date").isNotNull())
    .withColumn("delivery_status",
        F.when(F.col("delivery_date").isNotNull(), "Delivered").otherwise("In transit"))

    # split shipment context
    .withColumn("shipment_count_on_order", F.count("*").over(by_order))
    .withColumn("shipment_sequence", F.row_number().over(by_order_seq))
    .withColumn("is_split_shipment", F.col("shipment_count_on_order") > 1)
    .withColumn("is_final_shipment",
        F.col("shipment_sequence") == F.col("shipment_count_on_order"))
)

#display(fact_shipment_df.count())# count 15,369
# display(fact_shipment_df.groupBy("shipment_sk").agg(F.count("*").alias("count")).filter(F.col("count") > 1))

#fact_shipment_df.columns


# COMMAND ----------

# MAGIC %md
# MAGIC # write to gold table

# COMMAND ----------

write_to_gold(
    fact_shipment_df,
    shipment_gold_table,
    "t.shipment_sk=s.shipment_sk",
    [
        'shipment_number',
        'order_sk',
        'sales_order_number',
        'ship_to_customer_sk',
        'shipping_carrier',
        'order_date',
        'ship_date',
        'delivery_date',
        'batch_id',
        'order_date_id',
        'ship_date_id',
        'delivery_date_id',
        'order_to_ship_days',
        'transit_days',
        'order_to_delivery_days',
        'is_delivered',
       'delivery_status',
       'shipment_count_on_order',
       'shipment_sequence',
       'is_split_shipment',
       'is_final_shipment'
    ]
)