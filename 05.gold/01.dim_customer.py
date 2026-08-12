# Databricks notebook source
# MAGIC %md
# MAGIC ### Build Customer Dimension
# MAGIC 1. Exploring all tables with customer description
# MAGIC 2. Create dim_customer, joining tables on the right business key
# MAGIC 3. select only columns that distinctly describe the customer
# MAGIC 4. goal is to have a distinct count and description of the customer_id
# MAGIC 5. create a new customer_sk column using the hash function --> this later replaces the customer_id in each of the fact tables
# MAGIC 5. Write the transformed data to gold dim_customer table
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
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
# MAGIC # Exploring All tables in silver layer
# MAGIC  - Understand the grain of each table
# MAGIC  - Understand the business Key in each table
# MAGIC  - Explore table which shares similar business key
# MAGIC  - tables: cust_master. cities, address, customer_contacts, regions

# COMMAND ----------

cust_master_silver_table = f"{catalog_name}.{silver_schema}.cust_master"
cities_silver_table = f"{catalog_name}.{silver_schema}.cities"
address_silver_table = f"{catalog_name}.{silver_schema}.address"
customer_contacts_silver_table = f"{catalog_name}.{silver_schema}.customer_contacts"
regions_silver_table = f"{catalog_name}.{silver_schema}.regions"
dim_customer_table = f"{catalog_name}.{gold_schema}.dim_customer"


# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring all tables with customer description

# COMMAND ----------

# cust_master exploration
# business keys: customer_id, customer_city_id
# is customer_id unique
#cust_master_df = spark.read.table(cust_master_silver_table)
#cust_master_df.display()
#display(cust_master_df.groupBy("customer_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1))# customer_id not unique
#display(cust_master_df.groupBy(["customer_id", "customer_created_date"]).agg(F.count("*").alias("count")).filter(F.col("count") > 1))# grouping these attributes uniquely represents each customer
#display(cust_master_df.filter(F.col("customer_id")=="2441"))
# scope: for dim_customer: select the following columns: customer_id, customer_name, customer_segments, customer_city_id, customer_active_flag. A new customer_sk will be created in this df which later replaces the customer_id in each of the fact table




# exploring customer_contacts
#customer_contacts = spark.read.table(customer_contacts_silver_table)
#display(customer_contacts)
#display(customer_contacts.groupBy("customer_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1)) --> uniqueyl represent each row
# understanding the attribute, is_primary_contact. how many distinct value is present in this column?
#display(customer_contacts.select("is_primary_contact").distinct()): contains one distinct value, "Y"
#scope: from dim_customer_account: add each of the columns from customer_contact: customer_contact_name, customer_contact_email, is_primary_contact



# exploring address table:
#address_df = spark.read.table(address_silver_table)
#display(address_df)
# is customer_address_id unique?
#display(address_df.groupBy("customer_address_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1)) customer_address_id is unique
# is customer_id unique?
#display(address_df.groupBy(["customer_id", "customer_address_type"]).agg(F.count("*").alias("count")).filter(F.col("count") > 1)) #customer_id is not unique # combining customer_id and customer_address_type uniquely represents each row. customer_address type have two unique values (Bill_to and Ship_to)
# scope for building dim_customer and dim_customer_account: dim_customer will join address_df on customer_id where customer_address_type = ship_to(each of the following attributes will be retrievd and renamed: customer_street, customer_city_id, customer_postal_code). for dim_customer_account - we join on customer_id, where customer_address_type - bill_to(each of the following attributes will be retrieved and renamed: customer_bill_to_street, customer_bill_to_city_id, customer_bill_to_postal_code)
#display(address_df.filter(F.col("customer_id") == "2441"))


# exploring cities table:
#cities_df = spark.read.table(cities_silver_table)
#display(cities_df)
# is city_id unique?
#display(cities_df.groupBy("city_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1)) #city_id is unique
# retreieve each of the columns from cities_df into dim_customer: customer_city_name, customer_region_id, customer_region_name
# for dim_account_customer: retrieve the following and renamed: city_name: bill_to_city_name, bill_to_region_id, bill_to_region_name



# exploring region table:
#regions_df = spark.read.table(regions_silver_table)
#display(regions_df)
# is region_id unique?
#display(region_df.groupBy("region_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1))
# for dim_customer: retrieve: region_name.alias("region_full_name")
# for dim_customer_account: region_name.alias("region_full_name")




# COMMAND ----------

# MAGIC %md
# MAGIC # Creating dim_customer table

# COMMAND ----------

cust_master_df = spark.read.table(cust_master_silver_table)
#display(cust_master_df.count()) 5,100 count

# deduplicating
max_date_df = cust_master_df.groupBy(
    "customer_id"
).agg(
    F.max("customer_created_date").alias("max_created_date")
)

customer_df = cust_master_df.alias("c").join(
    max_date_df.alias("md"),
    (F.col("c.customer_id") == F.col("md.customer_id")) & 
    (F.col("c.customer_created_date") == F.col("md.max_created_date")),
    "inner"
).select(
    F.xxhash64(F.concat_ws("||", F.col("c.customer_id"), F.col("valid_from"))).alias("customer_sk"),
    F.col("c.customer_id"),
    F.col("c.customer_name"),
    F.col("c.customer_segment"),
    F.col("c.customer_city_id"),
    F.col("c.customer_active_flag"),
    F.col("valid_from"),
    F.col("valid_to"),
    F.col("is_current")
)

#display(customer_df.count()) # 5000 count

# customer_address
address_df = spark.read.table(address_silver_table)
#display(address_df.count()) --> 10,000 count(filter on customer_address_type returns 5000 count)
customer_address_df = address_df.select(
    F.col("customer_id"),
    F.col("customer_street"),
    F.col("customer_postal_code"),
    F.col("customer_address_type")
).filter(F.col("customer_address_type")=="SHIP_TO")
#display(customer_address_df)

# customer_city
cities_df = spark.read.table(cities_silver_table)
#display(cities_df)
customer_city_df = cities_df.select(
    F.col("city_id").alias("customer_city_id"),
    F.col("city_name").alias("customer_city_name"),
    F.col("region_id").alias("customer_region_id"),
    F.col("region_name").alias("customer_region")
)

# region
regions_df = spark.read.table(regions_silver_table)
#display(regions_df)
customer_region_df = regions_df.select(
    F.col("region_id").alias("customer_region_id"),
    F.col("region_name").alias("customer_region_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Create dim_customer, joining tables on the right business key
# MAGIC - select only columns that distinctly describe the customer
# MAGIC - goal is to have a distinct count and description of the customer_id

# COMMAND ----------

customer_final_df = customer_df.alias("c").join(
    customer_address_df.alias("ca"),
    F.col("c.customer_id") == F.col("ca.customer_id"),
    "inner"
).join(
    customer_city_df.alias("cc"),
    F.col("c.customer_city_id") == F.col("cc.customer_city_id"),
    "left"
).join(
    customer_region_df.alias("cr"),
    F.col("cc.customer_region_id") == F.col("cr.customer_region_id"),
    "left"
).select(
    F.col("customer_sk"), # add customer_sk
    F.col("c.customer_id"),
    F.col("c.customer_name"),
    F.col("c.customer_segment"),
    F.col("c.customer_city_id"),
    F.col("c.customer_active_flag"),
    F.col("ca.customer_street"),
    F.col("ca.customer_postal_code"),
    F.col("ca.customer_address_type"),
    F.col("cc.customer_city_name"),
    F.col("cc.customer_region"),
    F.col("cr.customer_region_id"),
    F.col("cr.customer_region_name"),
    F.col("valid_from"),
    F.col("valid_to"),
    F.col("is_current")
)
#display(customer_final_df)
#display(customer_final_df.count())
#customer_final_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC - Write the transformed data to gold dim_customer table

# COMMAND ----------

write_to_gold(
    customer_final_df,
    dim_customer_table,
    "t.customer_sk = s.customer_sk",
    [
        'customer_id',
        'customer_name',
        'customer_segment',
        'customer_city_id',
        'customer_active_flag',
        'customer_street',
        'customer_postal_code',
        'customer_address_type',
        'customer_city_name',
        'customer_region',
        'customer_region_id',
        'customer_region_name',
        "valid_from",
        "valid_to",
        "is_current"
    ]
)