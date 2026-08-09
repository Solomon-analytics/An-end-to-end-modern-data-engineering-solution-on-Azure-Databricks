# Databricks notebook source
# MAGIC %md
# MAGIC # Transform cities bronze data
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
# MAGIC # File: cities
# MAGIC  - define cities source file and silver table name using the environment variable
# MAGIC

# COMMAND ----------

bronze_cities_table = f"{catalog_name}.{bronze_schema}.cities"
silver_cities_table = f"{catalog_name}.{silver_schema}.cities"


# COMMAND ----------

# MAGIC %md
# MAGIC - Read file using spark dataframe reader API

# COMMAND ----------

cities_df = spark.read.table(bronze_cities_table).filter(F.col("batch_id")==v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC  - Keep only the columns required for analytics
# MAGIC  - Standardise all column headers using snake_case
# MAGIC  - Rename columns to business meaningful names

# COMMAND ----------

# rename country to region_name
cities_rename = cities_df.withColumnRenamed("country", "region_name")

# add new column region_full_name
cities_mapping = cities_rename.withColumn("region_full_name",
                                          F.when(F.col("region_name") == "US", "United States")
                                          .when(F.col("region_name") == "EU", "Europe")
                                          .when(F.col("region_name") == "APAC", "Asia Pacific")
                                          .when(F.col("region_name") == "LATAM", "Latin America")
                                          .when(F.col("region_name") == "ME", "Middle East & Africa")
                                          .otherwise(F.col("region_name")))

                                          

# COMMAND ----------

# MAGIC %md
# MAGIC  - setup loggings for audit purpose
# MAGIC  - Remove whitespaces from string columns
# MAGIC  - remove nulls from business keys
# MAGIC  - Remove duplicates

# COMMAND ----------

# setup logging for audit purpose
row_count = cities_mapping.count()

# remove white space from string columns
cities_rem_white_spaces = trim_whitespaces(cities_mapping)

# remove nulls from business key
# check if city_id uniiquely represent each row
#check_unique = cities_rem_white_spaces.groupBy("city_id").count().filter(F.col("count") > 1) #value in this column unqiquely represents each row 
# removing nulls from this column
#cities_rem_nulls = cities_rem_white_spaces.dropna(subset=["city_id"])
cities_rem_nulls = remove_nulls(cities_rem_white_spaces, ["city_id"])

print(f"before dropping na: {row_count} | after dropping na: {cities_rem_nulls.count()}")

# drop duplicates
cities_drop_dup = cities_rem_nulls.dropDuplicates(["city_id"])
print(f"before dropping duplicates: {row_count} | after dropping duplicates: {cities_drop_dup.count()}")




# COMMAND ----------

# MAGIC %md
# MAGIC  - Write transformed data to silver table

# COMMAND ----------

write_to_silver(
    cities_drop_dup,
    silver_cities_table,
    "t.city_id = s.city_id",
    [
        "city_name",
        "region_id",
        "region_name",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
        "region_full_name"
    ]

)