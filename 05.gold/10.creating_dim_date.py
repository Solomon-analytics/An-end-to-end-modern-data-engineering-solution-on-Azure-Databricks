# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-configuration
# MAGIC

# COMMAND ----------

dim_date_table = f"{catalog_name}.{gold_schema}.dim_date"

# COMMAND ----------

# define the calendar range
start_date = "2024-01-01"
end_date = "2027-12-31"
fiscal_start_month = 4

# COMMAND ----------

dim_date_df = (
    spark.sql(f"""
        SELECT explode( -- turn that array into row
            sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) -- sequence: build an array of every date between the defined start and end date
        ) AS full_date
    """)
    .withColumn("date_id", F.date_format("full_date", "yyyyMMdd")) ## format as date_id to match what is already present in each of the dim and fact tables

    .withColumn("day_of_month", F.dayofmonth("full_date")) ## create day of month, from 1 - 31
    .withColumn("day_name", F.date_format("full_date", "EEEE")) ## day name monday to sunday
    .withColumn("day_short_name", F.date_format("full_date", "EEE")) ## day name monday to sunday and make it short
    .withColumn("day_of_week", F.expr("((dayofweek(full_date) + 5) % 7) + 1")) 
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7)) ## define if it's either saturday or sunday

    .withColumn("week_of_year", F.weekofyear("full_date"))

    .withColumn("month_number", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("full_date", "MMM"))
    .withColumn("month_year", F.date_format("full_date", "yyyy-MM"))
    .withColumn("month_id", F.date_format("full_date", "yyyyMM"))
    .withColumn("month_start_date", F.trunc("full_date", "MM"))
    .withColumn("month_end_date", F.last_day("full_date"))

    .withColumn("quarter_number", F.quarter("full_date"))
    .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("full_date")))
    .withColumn("quarter_year",
        F.concat(F.year("full_date"), F.lit("-Q"), F.quarter("full_date")))

    .withColumn("calendar_year", F.year("full_date"))

    .withColumn("fiscal_year",
        F.when(F.month("full_date") >= fiscal_start_month, F.year("full_date"))
         .otherwise(F.year("full_date") - 1))
    .withColumn("fiscal_month_number",
        ((F.month("full_date") - fiscal_start_month + 12) % 12) + 1)
    .withColumn("fiscal_quarter_number",
        F.ceil((((F.month("full_date") - fiscal_start_month + 12) % 12) + 1) / 3))

    .withColumn("created_timestamp", F.current_timestamp()) # audit
    .withColumn("updated_timestamp", F.current_timestamp())
)

# COMMAND ----------

display(dim_date_df)

# COMMAND ----------

unknown_row = (
    dim_date_df.limit(1) # grab one row in the df
    .select(*[
        F.lit(None).cast(f.dataType).alias(f.name) # go through ever column in schema and replace its value with null, keeping the same name and type
        for f in dim_date_df.schema.fields
    ])
    .withColumn("date_id", F.lit("-1"))
    .withColumn("day_name", F.lit("Unknown"))
    .withColumn("month_name", F.lit("Unknown"))
    .withColumn("month_year", F.lit("Unknown"))
    .withColumn("quarter_name", F.lit("Unknown"))
    .withColumn("created_timestamp", F.current_timestamp())
    .withColumn("updated_timestamp", F.current_timestamp())
)## overwrites the nulls with the values in the unknown_row

dim_date_final_df = dim_date_df.unionByName(unknown_row) ## append the row


# COMMAND ----------

(dim_date_final_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(dim_date_table)) ## overwrite rather than merge, because it has no business key and icremental changes are not expected

print(f"{dim_date_table}: {dim_date_final_df.count():,} rows written")

# COMMAND ----------

display(spark.read.table(dim_date_table).count())

# COMMAND ----------

display(spark.read.table(dim_date_table)
        .groupBy("date_id").agg(F.count("*").alias("count"))
        .filter(F.col("count") > 1))

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS unmatched
    FROM   {catalog_name}.{gold_schema}.fact_sales_order f
    LEFT   JOIN {dim_date_table} d ON f.order_date_id = d.date_id
    WHERE  f.order_date_id IS NOT NULL AND d.date_id IS NULL
"""))    