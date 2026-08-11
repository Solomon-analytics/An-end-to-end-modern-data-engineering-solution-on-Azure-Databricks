# Databricks notebook source
# MAGIC %md
# MAGIC ### Build customer_account Dimension
# MAGIC 1. Exploring all tables with customer description
# MAGIC 2. Create dim_customer_account, joining tables on the right business key
# MAGIC 3. goal is to have additonal information about a customer which is not present in the dim_customer
# MAGIC 4. create account_created_date in date and string format
# MAGIC 5. create a bill_to_account_location_sk
# MAGIC 4. Write the transformed data to gold dim_customer table
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
dim_customer_account_table = f"{catalog_name}.{gold_schema}.dim_customer_account"


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
# scope: for dim_customer_account: select the following columns and renamed: customer_name:account_name, customer_segment:account_segment, customer_city_id:account_city_id, customer_credit_limit:account_credit_limit, customer_payment_terms:account_payment_terms, customer_account_manager:account_manager, customer_city_id:account_city_id, customer_active_flag:account_active_flag, customer_created_date:account_created_date

# better understanding the grain: customer_id + customer_created_date
#display(cust_master_df.groupBy("customer_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1) )
#display(cust_master_df.filter(F.col("customer_id").isin("5820","3962", "1146", "4491")))

# There are 100 occurence where a customer account was updated with a different customer_created_at, rather than setting a new customer_id, it retains the same customer_id. Rather than deduplicating this, we add a account_is_update_flag




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
# MAGIC %md
# MAGIC # Creating dim_customer_account table

# COMMAND ----------

customer_account_df = spark.read.table(cust_master_silver_table).filter(F.col("is_current"))
#customer_account_df.columns

# Add a flag to identify if this is an updated account (same customer_id with later customer_created_date)
max_created_dates = customer_account_df.groupBy("customer_id").agg(F.max("customer_created_date").alias("max_created_date"))
customer_account_df = customer_account_df.join(max_created_dates, "customer_id", "left")
customer_account_df = customer_account_df.withColumn(
    "account_is_update",
    F.when(F.col("customer_created_date") == F.col("max_created_date"), "N").otherwise("Y")
).filter(F.col("account_is_update")=="N").withColumn(
    "customer_account_created_date",
    F.to_date(F.col("customer_created_date"))).withColumn(
    "customer_account_created_date_id",
    F.date_format(F.col("customer_account_created_date"), "yyyyMMdd")
)


#display(customer_account_df)

# applying filter on account_is_update to only return latest account information
customer_account_df = customer_account_df

# selecting columns
customer_account_selected_df = customer_account_df.select(
 F.col('customer_id').alias("account_id"),
 F.col('customer_name').alias("account_name"),
 F.col('customer_segment').alias("account_segment"),
 F.col('customer_credit_limit').alias("account_credit_limit"),
 F.col('customer_payment_terms').alias("account_payment_terms"),
 F.col('customer_account_manager').alias("account_manager"),
 F.col('customer_city_id').alias("account_city_id"),
 F.col("customer_account_created_date"),
 F.col("customer_account_created_date_id"),
 F.col('customer_active_flag').alias("account_active_flag"),
 F.col("account_is_update")
)

#display(customer_account_selected_df)

# account_address
address_df = spark.read.table(address_silver_table)
#display(address_df.count()) --> 10,000 count(filter on customer_address_type returns 5000 count)
account_address_df = address_df.select(
    F.col("customer_id").alias("account_id"),
    F.col("customer_street").alias("account_bill_to_street"),
    F.col("customer_postal_code").alias("account_bill_to_postal_code"),
    F.col("customer_address_type").alias("account_bill_to_address_type")
).filter(F.col("customer_address_type")=="BILL_TO")
#display(customer_address_df)


# account_city
cities_df = spark.read.table(cities_silver_table)
#display(cities_df)
account_city_df = cities_df.select(
    F.col("city_id").alias("account_city_id"),
    F.col("city_name").alias("account_city_name"),
    F.col("region_id").alias("account_region_id"),
    F.col("region_name").alias("account_region")
)


# region
regions_df = spark.read.table(regions_silver_table)
#display(regions_df)
account_region_df = regions_df.select(
    F.col("region_id").alias("account_region_id"),
    F.col("region_name").alias("account_region_name"))




# COMMAND ----------

# MAGIC %md
# MAGIC - Create dim_customer_account, joining tables on the right business key
# MAGIC - goal is to have additonal information about a customer which is not present in the dim_customer

# COMMAND ----------

customer_account_final_df = customer_account_selected_df.alias("acc").join(
    account_address_df.alias("aa"),
    F.col("acc.account_id") == F.col("aa.account_id"),
    "inner"
).join(
    account_city_df.alias("ac"),
    F.col("acc.account_city_id") == F.col("ac.account_city_id"),
    "left"
).join(
    account_region_df.alias("ar"),
    F.col("ac.account_region_id") == F.col("ar.account_region_id"),
    "left"
).select(
    F.xxhash64(F.col("acc.account_id")).alias("bill_to_account_sk"), # add bill_to_account_sk
    F.col("acc.account_id"),
    F.col("acc.account_name"),
    F.col("acc.account_segment"),
    F.col('acc.account_credit_limit'),
    F.col('acc.account_payment_terms'),
    F.col('acc.account_manager'),
    F.col('acc.account_city_id'),
    F.col("acc.customer_account_created_date"),
    F.col("customer_account_created_date_id"),
    F.col("acc.account_active_flag"),
    
    F.col("aa.account_bill_to_postal_code"),
    F.col("aa.account_bill_to_address_type"),
    F.col("ac.account_city_name"),
    F.col("ac.account_region"),
    F.col("ar.account_region_id"),
    F.col("ar.account_region_name")
)
#display(customer_account_final_df)# returns a count of 5000
#customer_account_final_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # Write the transformed data to gold dim_customer_account table
# MAGIC

# COMMAND ----------

write_to_gold(
    customer_account_final_df,
    dim_customer_account_table,
    "t.bill_to_account_sk = s.bill_to_account_sk",
    [
        'account_id',
        'account_name',
        'account_segment',
        'account_credit_limit',
        'account_payment_terms',
        'account_manager',
        'account_city_id',
        'customer_account_created_date',
        'customer_account_created_date_id',
        'account_active_flag',
        'account_bill_to_postal_code',
        'account_bill_to_address_type',
        'account_city_name',
        'account_region',
        'account_region_id',
        'account_region_name'
    ]
)